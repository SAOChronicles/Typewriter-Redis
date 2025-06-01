
package com.typewritermc.engine.paper.facts.storage

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.typewritermc.engine.paper.entry.entries.GroupId
import com.typewritermc.engine.paper.facts.FactData
import com.typewritermc.engine.paper.facts.FactId
import com.typewritermc.engine.paper.facts.FactStorage
import com.typewritermc.engine.paper.logger
import com.typewritermc.engine.paper.plugin
import com.typewritermc.engine.paper.utils.get
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.koin.core.component.KoinComponent
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.io.FileNotFoundException
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

//ss
class RedisFactStorage : FactStorage, KoinComponent {
    private lateinit var jedisPool: JedisPool
    private val config: RedisConfig

    private val gson: Gson = GsonBuilder()
        .setPrettyPrinting()
        .registerTypeAdapter(LocalDateTime::class.java, LocalDateTimeSerializer)
        .create()

    init {
        config = loadConfig()
    }

    override fun init() {
        super.init()
        val poolConfig = JedisPoolConfig().apply {
            maxTotal = 10
            maxIdle = 5
            minIdle = 1
            testOnBorrow = true
            testOnReturn = true
            testWhileIdle = true
            minEvictableIdleTimeMillis = TimeUnit.MINUTES.toMillis(60)
            timeBetweenEvictionRunsMillis = TimeUnit.SECONDS.toMillis(30)
            numTestsPerEvictionRun = 3
            blockWhenExhausted = true
        }

        jedisPool = if (config.password != null && config.password.isNotEmpty()) {
            JedisPool(poolConfig, config.host, config.port, 2000, config.password, config.database)
        } else {
            JedisPool(poolConfig, config.host, config.port, 2000, null, config.database)
        }

        logger.info("Redis fact storage initialized with host: ${config.host}, port: ${config.port}, database: ${config.database}")
    }

    override fun shutdown() {
        if (::jedisPool.isInitialized && !jedisPool.isClosed) {
            jedisPool.close()
            logger.info("Redis fact storage shutdown")
        }
    }

    override suspend fun loadFacts(): Map<FactId, FactData> = withContext(Dispatchers.IO) {
        val facts = mutableMapOf<FactId, FactData>()

        try {
            jedisPool.resource.use { jedis ->
                val keys = jedis.keys("fact:*")
                if (keys.isNotEmpty()) {
                    val values = jedis.mget(*keys.toTypedArray())
                    keys.zip(values).forEach { (key, value) ->
                        if (value != null) {
                            try {
                                val parts = key.split(":")
                                if (parts.size == 3) {
                                    val entryId = parts[1]
                                    val groupId = parts[2]
                                    val factId = FactId(entryId, GroupId(groupId))
                                    val factData = gson.fromJson(value, FactData::class.java)
                                    facts[factId] = factData
                                }
                            } catch (e: Exception) {
                                logger.warning("Erreur lors de l'analyse des données de fact pour la clé $key: ${e.message}")
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Erreur lors du chargement des facts depuis Redis: ${e.message}")
            e.printStackTrace()
        }

        facts
    }

    override suspend fun storeFacts(facts: Collection<Pair<FactId, FactData>>) = withContext(Dispatchers.IO) {
        if (facts.isEmpty()) return@withContext

        try {
            jedisPool.resource.use { jedis ->
                val existingKeys = jedis.keys("fact:*")
                val keysToKeep = mutableSetOf<String>()
                jedis.pipelined().use { pipeline ->
                    facts.forEach { (id, data) ->
                        val key = "fact:${id.entryId}:${id.groupId.id}"
                        keysToKeep.add(key)
                        val jsonData = gson.toJson(data)
                        pipeline.set(key, jsonData)
                    }
                    pipeline.sync()
                }
                val keysToDelete = existingKeys.filter { it !in keysToKeep }
                if (keysToDelete.isNotEmpty()) {
                    jedis.del(*keysToDelete.toTypedArray())
                }
            }
        } catch (e: Exception) {
            logger.severe("Erreur lors de l'enregistrement des facts dans Redis: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun loadConfig(): RedisConfig {
        val configFile = plugin.dataFolder["redis.yml"]

        if (!configFile.exists()) {
            logger.warning("Le fichier de configuration Redis est manquant. Utilisation des valeurs par défaut.")
            return RedisConfig(
                host = "",
                port = 0,
                password = "",
                database = 0
            )
        }

        val yamlConfig = org.bukkit.configuration.file.YamlConfiguration.loadConfiguration(configFile)

        val host = yamlConfig.getString("host") ?: ""
        val port = yamlConfig.getInt("port")
        val database = yamlConfig.getInt("database")
        val password = yamlConfig.getString("password")

        return RedisConfig(
            host = host,
            port = port,
            password = password,
            database = database
        )
    }
}

data class RedisConfig(
    val host: String,
    val port: Int,
    val password: String? = null,
    val database: Int
)