package com.typewritermc.engine.paper.facts.storage

import com.typewritermc.engine.paper.facts.FactData
import com.typewritermc.engine.paper.facts.FactId
import com.typewritermc.engine.paper.facts.FactStorage
import com.typewritermc.engine.paper.logger

class CompositeFactStorage(
    private val redisStorage: RedisFactStorage,
    private val fileStorage: FileFactStorage
) : FactStorage {

    override fun init() {
        redisStorage.init()
        fileStorage.init()
        logger.info("CompositeFactStorage initialisé avec Redis et FileStorage")
    }

    override fun shutdown() {
        redisStorage.shutdown()
        fileStorage.shutdown()
        logger.info("CompositeFactStorage arrêté")
    }

    override suspend fun loadFacts(): Map<FactId, FactData> {
        val redisFacts = try {
            redisStorage.loadFacts()
        } catch (e: Exception) {
            logger.warning("Erreur lors du chargement des facts depuis Redis: ${e.message}")
            emptyMap()
        }


        val fileFacts = try {
            fileStorage.loadFacts()
        } catch (e: Exception) {
            logger.warning("Erreur lors du chargement des facts depuis le système de fichiers: ${e.message}")
            emptyMap()
        }
        val mergedFacts = fileFacts.toMutableMap()
        mergedFacts.putAll(redisFacts)

        return mergedFacts
    }

    override suspend fun storeFacts(facts: Collection<Pair<FactId, FactData>>) {
        try {
            redisStorage.storeFacts(facts)
        } catch (e: Exception) {
            logger.warning("Erreur lors du stockage des facts dans Redis: ${e.message}")
        }
        try {
            fileStorage.storeFacts(facts)
        } catch (e: Exception) {
            logger.warning("Erreur lors du stockage des facts dans le système de fichiers: ${e.message}")
        }
    }
}