-- ==============================================================================
-- File : match_making.lua (Alternative A)
-- Description : Matchmaking that partially follows the DIRAC schema.
-- Note : Evaluation of complex boolean Tag expressions is omitted
-- because it is too expensive/complex for vanilla Lua (this is Alt A's limit).
--
-- Input arguments (from Python) :
-- KEYS[1] : The queue name (e.g. 'jobs:pending')
-- KEYS[2] : The prefix for job keys (e.g. 'job:')
-- ARGV[1] : Available RAM on the node (integer)
-- ARGV[2] : Available CPUs on the node (integer)
-- ARGV[3] : Current node site (string, e.g. 'LCG.CERN.ch')
-- ARGV[4] : Batch size (integer, e.g. 100) -> maximum number of jobs to test
-- ==============================================================================

local queue_key = KEYS[1]
local job_prefix = KEYS[2]

local node_ram = tonumber(ARGV[1])
local node_cores = tonumber(ARGV[2])
local node_walltime = tonumber(ARGV[3])
local node_site = ARGV[4]
local batch_size = tonumber(ARGV[5])

-- Fetch IDs (FIFO: oldest to newest)
local top_jobs = redis.call('ZRANGE', queue_key, 0, batch_size - 1)

if #top_jobs == 0 then
    return nil
end

for _, job_id in ipairs(top_jobs) do
    local job_key = job_prefix .. job_id

    local job_reqs = redis.call('HMGET', job_key,
        'min_cores', 'max_cores',
        'ram_overhead', 'ram_per_core',
        'walltime', 'site'
    )

    if job_reqs[1] then
        local j_min_cores = tonumber(job_reqs[1]) or 1
        local j_max_cores = tonumber(job_reqs[2]) or j_min_cores
        local j_ram_overhead = tonumber(job_reqs[3]) or 0
        local j_ram_per_core = tonumber(job_reqs[4]) or 0
        local j_walltime = tonumber(job_reqs[5]) or 0
        local j_site = job_reqs[6]

        -- 1. Site check
        if j_site == 'ANY' or j_site == node_site then
            -- 2. Walltime check
            if node_walltime >= j_walltime then
                -- 3. Core check (does the pilot have at least the required minimum?)
                if node_cores >= j_min_cores then
                    -- Assign the maximum possible cores without exceeding the job request
                    local assigned_cores = math.min(node_cores, j_max_cores)

                    -- 4. RAM check (dynamic calculation according to the schema)
                    local required_ram = j_ram_overhead + (j_ram_per_core * assigned_cores)

                    if node_ram >= required_ram then
                        -- IT'S A MATCH!
                        redis.call('ZREM', queue_key, job_id)

                        -- Return a JSON string with the ID and allocated cores
                        return '{"job_id": "' .. job_id .. '", "assigned_cores": ' .. assigned_cores .. '}'
                    end
                end
            end
        end
    end
end

return nil
