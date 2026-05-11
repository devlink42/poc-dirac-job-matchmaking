-- ==============================================================================
-- Fichier : match_making.lua
-- Description : Trouve et assigne le job le plus prioritaire compatible avec un Node.
--
-- Arguments d'entrée (depuis Python) :
-- KEYS[1] : Le nom de la file d'attente (ex: 'jobs:pending')
-- KEYS[2] : Le préfixe des clés de jobs (ex: 'job:')
-- ARGV[1] : La RAM disponible sur le Node (entier)
-- ARGV[2] : Les CPUs disponibles sur le Node (entier)
-- ARGV[3] : Le site actuel du Node (string, ex: 'LCG.CERN.ch')
-- ARGV[4] : La taille du batch (entier, ex: 100) -> combien de jobs on teste max
-- ==============================================================================

local queue_key = KEYS[1]
local job_prefix = KEYS[2]

local node_ram = tonumber(ARGV[1])
local node_cpu = tonumber(ARGV[2])
local node_site = ARGV[3]
local batch_size = tonumber(ARGV[4])

-- 1. Récupérer les identifiants des N jobs les plus prioritaires
-- ZREVRANGE récupère de la priorité la plus haute à la plus basse (index 0 à batch_size-1)
local top_jobs = redis.call('ZREVRANGE', queue_key, 0, batch_size - 1)

-- Si la file est vide, on arrête là
if #top_jobs == 0 then
    return nil
end

-- 2. Boucler sur les jobs récupérés (ipairs permet d'itérer sur un tableau Lua)
for _, job_id in ipairs(top_jobs) do
    local job_key = job_prefix .. job_id

    -- HMGET récupère plusieurs champs d'un HASH d'un seul coup
    local job_reqs = redis.call('HMGET', job_key, 'req_ram', 'req_cpu', 'target_site')

    -- Attention : HMGET renvoie un tableau avec 'false' si la clé ou le champ n'existe pas
    if job_reqs[1] then
        local j_ram = tonumber(job_reqs[1]) or 0
        local j_cpu = tonumber(job_reqs[2]) or 0
        local j_site = job_reqs[3]

        -- 3. Logique de "Match"
        -- Est-ce que le nœud a assez de RAM et de CPU ?
        if node_ram >= j_ram and node_cpu >= j_cpu then
            -- Est-ce que le site correspond (ou si le job accepte n'importe où 'ANY') ?
            if j_site == 'ANY' or j_site == node_site then
                -- C'EST UN MATCH !
                -- On le retire de la file pour que personne d'autre ne le prenne
                redis.call('ZREM', queue_key, job_id)

                -- On change son statut dans son HASH (optionnel mais recommandé)
                redis.call('HSET', job_key, 'status', 'assigned', 'assigned_to', node_site)

                -- On renvoie l'ID au Pilot
                return job_id
            end
        end
    end
end

-- Aucun job ne correspond dans le batch testé
return nil
