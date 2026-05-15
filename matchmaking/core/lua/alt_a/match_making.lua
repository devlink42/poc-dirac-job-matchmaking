-- ==============================================================================
-- File : match_making.lua (Alternative A)
-- Description : Matchmaking that partially follows the DIRAC schema.
-- Note : Evaluation of complex boolean Tag expressions is omitted
-- because it is too expensive/complex for vanilla Lua (this is Alt A's limit).
--
-- Input arguments (from Python) :
-- ARGV[1] : node_ram
-- ARGV[2] : node_cores
-- ARGV[3] : node_site
-- ARGV[4] : candidates_count
-- ARGV[5] : node_system_name
-- ARGV[6] : node_system_glibc
-- ARGV[7] : node_system_userns
-- ARGV[8] : node_wall_time
-- ARGV[9] : node_cpu_work
-- ARGV[10] : node_arch_name
-- ARGV[11] : node_arch_level
-- ARGV[12] : node_gpu_count
-- ARGV[13] : node_gpu_ram
-- ARGV[14] : node_gpu_vendor
-- ARGV[15] : node_gpu_compute
-- ARGV[16] : node_gpu_driver
-- ARGV[17] : node_io_scratch
-- ==============================================================================

local queue_key = KEYS[1]
local job_prefix = KEYS[2]

local node_ram = tonumber(ARGV[1]) or 0
local node_cores = tonumber(ARGV[2]) or 0
local node_site = ARGV[3]
local batch_size = tonumber(ARGV[4]) or 100

local node_sys_name = ARGV[5]
local node_sys_glibc = ARGV[6]
local node_sys_userns = tonumber(ARGV[7]) or 0

local node_wall_time = tonumber(ARGV[8]) or 0
local node_cpu_work = tonumber(ARGV[9]) or 0

local node_arch_name = ARGV[10]
local node_arch_level = tonumber(ARGV[11]) or 0

local node_gpu_count = tonumber(ARGV[12]) or 0
local node_gpu_ram = tonumber(ARGV[13]) or 0
local node_gpu_vendor = ARGV[14]
local node_gpu_compute = ARGV[15]
local node_gpu_driver = ARGV[16]

local node_io_scratch = tonumber(ARGV[17]) or 0

local function cmp_version(v1, v2)
    if not v1 or not v2 or v1 == "" or v2 == "" then return 0 end

    local p1, p2 = {}, {}
    for part in string.gmatch(v1, "%d+") do table.insert(p1, tonumber(part)) end
    for part in string.gmatch(v2, "%d+") do table.insert(p2, tonumber(part)) end

    local len = math.max(#p1, #p2)
    for i = 1, len do
        local n1 = p1[i] or 0
        local n2 = p2[i] or 0
        if n1 < n2 then return -1 end
        if n1 > n2 then return 1 end
    end

    return 0
end

local top_jobs = redis.call('ZRANGE', queue_key, 0, batch_size - 1)
if #top_jobs == 0 then return nil end

for _, job_id in ipairs(top_jobs) do
    local job_key = job_prefix .. job_id

    local job_reqs = redis.call('HMGET', job_key,
        'cpu_num_cores_min', 'cpu_num_cores_max', 'cpu_ram_mb_request_overhead', 'cpu_ram_mb_request_per_core',
        'cpu_ram_mb_limit_overhead', 'cpu_ram_mb_limit_per_core', 'site', 'system_name', 'system_glibc',
        'system_user_namespaces', 'wall_time', 'cpu_work', 'cpu_architecture_name',
        'cpu_architecture_microarchitecture_level_min', 'cpu_architecture_microarchitecture_level_max', 'gpu_count_min',
        'gpu_count_max', 'gpu_ram_mb', 'gpu_vendor', 'gpu_compute_capability_min', 'gpu_compute_capability_max',
        'gpu_driver_version', 'io_scratch_mb'
    )

    if job_reqs[1] then
        local j_min_cores = tonumber(job_reqs[1]) or 1
        local j_max_cores = tonumber(job_reqs[2]) or j_min_cores

        local valid = true
        if node_cores < j_min_cores then valid = false end
        if valid and job_reqs[7] and job_reqs[7] ~= 'ANY' and job_reqs[7] ~= node_site then valid = false end
        if valid and job_reqs[8] and job_reqs[8] ~= node_sys_name then valid = false end
        if valid and job_reqs[9] and cmp_version(node_sys_glibc, job_reqs[9]) < 0 then valid = false end
        if valid and job_reqs[10] == "1" and node_sys_userns == 0 then valid = false end

        local j_walltime = tonumber(job_reqs[11])
        if valid and j_walltime and node_wall_time < j_walltime then valid = false end

        local j_cpuwork = tonumber(job_reqs[12])
        if valid and j_cpuwork and node_cpu_work < j_cpuwork then valid = false end

        if valid and job_reqs[13] and job_reqs[13] ~= node_arch_name then valid = false end

        local j_arch_min = tonumber(job_reqs[14])
        if valid and j_arch_min and node_arch_level < j_arch_min then valid = false end

        local j_arch_max = tonumber(job_reqs[15])
        if valid and j_arch_max and node_arch_level > j_arch_max then valid = false end

        local j_gpu_min = tonumber(job_reqs[16])
        if valid and j_gpu_min then
            if node_gpu_count < j_gpu_min then valid = false end

            local j_gpu_max = tonumber(job_reqs[17])
            if valid and j_gpu_max and node_gpu_count > j_gpu_max then valid = false end

            if valid and node_gpu_count > 0 then
                local j_gpu_ram = tonumber(job_reqs[18])
                if valid and j_gpu_ram and node_gpu_ram < j_gpu_ram then valid = false end
                if valid and job_reqs[19] and job_reqs[19] ~= node_gpu_vendor then valid = false end
                if valid and job_reqs[20] and cmp_version(node_gpu_compute, job_reqs[20]) < 0 then valid = false end
                if valid and job_reqs[21] and cmp_version(node_gpu_compute, job_reqs[21]) > 0 then valid = false end
                if valid and job_reqs[22] and cmp_version(node_gpu_driver, job_reqs[22]) < 0 then valid = false end
            end
        end

        local j_io_scratch = tonumber(job_reqs[23])
        if valid and j_io_scratch and node_io_scratch < j_io_scratch then valid = false end

        local assigned_cores = math.min(node_cores, j_max_cores)

        if valid then
            local j_ram_req_oh = tonumber(job_reqs[3]) or 0
            local j_ram_req_pc = tonumber(job_reqs[4]) or 0
            local req_ram = j_ram_req_oh + (j_ram_req_pc * j_min_cores)

            if node_ram < req_ram then valid = false end

            if valid then
                local j_ram_lim_oh = tonumber(job_reqs[5]) or 0
                local j_ram_lim_pc = tonumber(job_reqs[6]) or 0

                if j_ram_lim_oh > 0 or j_ram_lim_pc > 0 then
                    local lim_ram = j_ram_lim_oh + (j_ram_lim_pc * j_min_cores)
                    if node_ram < lim_ram then valid = false end
                end
            end
        end

        if valid then
            redis.call('ZREM', queue_key, job_id)

            return '{"job_id": "' .. job_id .. '", "assigned_cores": ' .. assigned_cores .. '}'
        end
    end
end

return nil
