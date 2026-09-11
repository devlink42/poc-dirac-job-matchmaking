--[[
Atomically select and reserve one job from an Alternative C site-set queue.

Invoke this script with no KEYS and these arguments:
    ARGV[1]: pilot site
    ARGV[2]: requested job type
    ARGV[3]: pilot architecture
    ARGV[4]: pilot GPU flag ("0" or "1")
    ARGV[5]: available RAM in MB
    ARGV[6]: available CPU cores

The script returns the oldest eligible job ID, or nil when none is available.
Tags are intentionally outside the scope of this initial implementation.

All Redis keys are derived at runtime. Consequently, this script targets a
standalone Redis deployment or a single shard. It is not Redis Cluster safe:
EVAL keys must be declared up front and every key touched by one invocation
must share a hash slot in a clustered deployment.
]]

local function parse_non_negative_integer(value)
    if type(value) ~= "string" or string.match(value, "^%d+$") == nil then
        return nil
    end

    local parsed_value = tonumber(value)
    if parsed_value == nil or parsed_value ~= parsed_value or parsed_value == math.huge then
        return nil
    end

    return parsed_value
end

local function list_to_hash(values)
    local result = {}

    for index = 1, #values, 2 do
        result[values[index]] = values[index + 1]
    end

    return result
end

local function decode_eligible_sites(encoded_sites)
    if type(encoded_sites) ~= "string" or encoded_sites == "" then
        return nil
    end

    local decoded, sites = pcall(cjson.decode, encoded_sites)
    if not decoded or type(sites) ~= "table" then
        return nil
    end

    local site_count = 0
    local highest_index = 0

    for index, site in pairs(sites) do
        if type(index) ~= "number" or index < 1 or index % 1 ~= 0 then
            return nil
        end

        if type(site) ~= "string" or site == "" then
            return nil
        end

        site_count = site_count + 1
        highest_index = math.max(highest_index, index)
    end

    if site_count == 0 or highest_index ~= site_count then
        return nil
    end

    return sites
end

local function contains_site(sites, expected_site)
    for _, site in ipairs(sites) do
        if site == "__any_site__" or site == expected_site then
            return true
        end
    end

    return false
end

local function index_key(site, job_type, architecture, has_gpu)
    return "index:" .. site .. ":" .. job_type .. ":" .. architecture .. ":" .. has_gpu
end

local function remove_index_reference(key, req_group_id)
    redis.pcall("SREM", key, req_group_id)
end

local function delete_empty_group(req_group_id, group, eligible_sites, current_index_key)
    local deleted_indexes = {}

    for _, eligible_site in ipairs(eligible_sites) do
        local group_index_key = index_key(
            eligible_site,
            group.job_type,
            group.architecture,
            group.has_gpu
        )

        if not deleted_indexes[group_index_key] then
            remove_index_reference(group_index_key, req_group_id)
            deleted_indexes[group_index_key] = true
        end
    end

    if not deleted_indexes[current_index_key] then
        remove_index_reference(current_index_key, req_group_id)
    end

    redis.call("DEL", "queue:" .. req_group_id, "req_group:" .. req_group_id)
end

if #KEYS ~= 0 or #ARGV ~= 6 then
    return nil
end

local pilot_site = ARGV[1]
local requested_job_type = ARGV[2]
local pilot_architecture = ARGV[3]
local pilot_has_gpu = ARGV[4]
local available_ram_mb = parse_non_negative_integer(ARGV[5])
local available_cores = parse_non_negative_integer(ARGV[6])

if pilot_site == "" or requested_job_type == "" or pilot_architecture == "" then
    return nil
end

if pilot_has_gpu ~= "0" and pilot_has_gpu ~= "1" then
    return nil
end

if available_ram_mb == nil or available_cores == nil then
    return nil
end

local index_keys = {
    index_key(pilot_site, requested_job_type, pilot_architecture, pilot_has_gpu),
    index_key("__any_site__", requested_job_type, pilot_architecture, pilot_has_gpu)
}
local group_ids = {}
local seen_group_ids = {}

for _, candidate_index_key in ipairs(index_keys) do
    local candidate_group_ids = redis.pcall("SMEMBERS", candidate_index_key)
    if type(candidate_group_ids) == "table" and candidate_group_ids.err == nil then
        for _, req_group_id in ipairs(candidate_group_ids) do
            if not seen_group_ids[req_group_id] then
                seen_group_ids[req_group_id] = true
                table.insert(group_ids, req_group_id)
            end
        end
    end
end

for _, req_group_id in ipairs(group_ids) do
    local group_key = "req_group:" .. req_group_id
    local hash_values = redis.pcall("HGETALL", group_key)

    if type(hash_values) ~= "table" or hash_values.err ~= nil then
        for _, candidate_index_key in ipairs(index_keys) do
            remove_index_reference(candidate_index_key, req_group_id)
        end
    elseif #hash_values == 0 then
        for _, candidate_index_key in ipairs(index_keys) do
            remove_index_reference(candidate_index_key, req_group_id)
        end
    else
        local group = list_to_hash(hash_values)
        local min_ram_mb = parse_non_negative_integer(group.min_ram_mb)
        local min_cpu_cores = parse_non_negative_integer(group.min_cpu_cores)
        local eligible_sites = decode_eligible_sites(group.eligible_sites)
        local valid_group = min_ram_mb ~= nil
            and min_cpu_cores ~= nil
            and group.job_type == requested_job_type
            and group.architecture == pilot_architecture
            and group.has_gpu == pilot_has_gpu
            and eligible_sites ~= nil
            and contains_site(eligible_sites, pilot_site)

        if not valid_group then
            for _, candidate_index_key in ipairs(index_keys) do
                remove_index_reference(candidate_index_key, req_group_id)
            end
        elseif available_ram_mb >= min_ram_mb and available_cores >= min_cpu_cores then
            local queue_key = "queue:" .. req_group_id
            local job_id = redis.pcall("RPOP", queue_key)

            if type(job_id) == "table" and job_id.err ~= nil then
                remove_index_reference(current_index_key, req_group_id)
            else
                if job_id == false or job_id == nil then
                    delete_empty_group(req_group_id, group, eligible_sites, index_keys[1])
                elseif redis.call("LLEN", queue_key) == 0 then
                    delete_empty_group(req_group_id, group, eligible_sites, index_keys[1])
                end

                if type(job_id) == "string" and job_id ~= "" then
                    return job_id
                end
            end
        end
    end
end

return nil
