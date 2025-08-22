#!/usr/bin/env node

import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';

// Configuration
const CONFIG = {
  // Test modes: 'duckdb', 'sqlite', 'postgres', 'all'
  mode: process.env.TEST_MODE || 'all',

  // Query interval in milliseconds
  queryInterval: parseInt(process.env.QUERY_INTERVAL) || 5,

  // Number of iterations (0 = infinite)
  maxIterations: parseInt(process.env.MAX_ITERATIONS) || 0,

  // Memory monitoring interval (log every N iterations)
  memoryLogInterval: parseInt(process.env.MEMORY_LOG_INTERVAL) || 250,

  // PostgreSQL connection settings
  postgres: {
    host: process.env.PG_HOST || '127.0.0.1',
    port: process.env.PG_PORT || '5432',
    dbname: process.env.PG_DB || 'duckdb_memory_test',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || ''
  },

  // SQLite file path
  sqliteFile: process.env.SQLITE_FILE || './test_memory.db',

  // DuckDB settings
  duckdb: {
    memory_limit: process.env.DUCKDB_MEMORY_LIMIT || '500MB',
    threads: process.env.DUCKDB_THREADS || '4'
  },

  // Force garbage collection
  forceGC: process.env.FORCE_GC === 'true',

  // Create new connection for each query
  newConnectionPerQuery: process.env.NEW_CONNECTION === 'true'
};

// Global variables
let duckdbInstance = null;
let connection = null;
let iterationCount = 0;
let startTime = Date.now();

// Memory tracking
const memoryStats = [];

class MemoryLeakTester {
  constructor() {
    this.setupSignalHandlers();
  }

  setupSignalHandlers() {
    process.on('SIGINT', () => {
      console.log('\n🛑 Received SIGINT, shutting down gracefully...');
      this.cleanup();
      process.exit(0);
    });

    process.on('SIGTERM', () => {
      console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
      this.cleanup();
      process.exit(0);
    });
  }

  async cleanup() {
    console.log('🧹 Cleaning up resources...');

    if (connection) {
      try {
        connection.disconnectSync();
        console.log('✅ DuckDB connection closed');
      } catch (error) {
        console.error('❌ Error closing connection:', error.message);
      }
    }

    if (duckdbInstance) {
      try {
        // DuckDB instance cleanup happens automatically
        console.log('✅ DuckDB instance cleaned up');
      } catch (error) {
        console.error('❌ Error cleaning up instance:', error.message);
      }
    }

    // Write memory stats to file
    this.saveMemoryStats();
  }

  saveMemoryStats() {
    if (memoryStats.length > 0) {
      const filename = `memory_stats_${CONFIG.mode}_${Date.now()}.json`;
      fs.writeFileSync(filename, JSON.stringify({
        config: CONFIG,
        stats: memoryStats,
        summary: {
          duration: Date.now() - startTime,
          iterations: iterationCount,
          initialMemory: memoryStats[0],
          finalMemory: memoryStats[memoryStats.length - 1]
        }
      }, null, 2));
      console.log(`📊 Memory stats saved to ${filename}`);
    }
  }

  async setupDuckDB() {
    console.log('🦆 Setting up DuckDB...');

    duckdbInstance = await DuckDBInstance.create(':memory:', {
      memory_limit: CONFIG.duckdb.memory_limit,
      threads: CONFIG.duckdb.threads
    });

    if (!CONFIG.newConnectionPerQuery) {
      connection = await duckdbInstance.connect();
    }

    console.log('✅ DuckDB setup complete');
  }

  async setupPostgreSQL() {
    console.log('🐘 Setting up PostgreSQL connection...');

    const connString = `host=${CONFIG.postgres.host} port=${CONFIG.postgres.port} dbname=${CONFIG.postgres.dbname} user=${CONFIG.postgres.user}`;
    const connStringWithPassword = CONFIG.postgres.password
      ? `${connString} password=${CONFIG.postgres.password}`
      : connString;

    const conn = CONFIG.newConnectionPerQuery
      ? await duckdbInstance.connect()
      : connection;

    try {
      // Install and load postgres extension
      await conn.run("INSTALL postgres;");
      await conn.run("LOAD postgres;");

      // Attach PostgreSQL database
      await conn.run(`ATTACH '${connStringWithPassword}' AS postgres_db (TYPE postgres);`);

      // Create test table if it doesn't exist
      await conn.run(`
        CREATE TABLE IF NOT EXISTS postgres_db.memory_test_table (
          id BIGINT PRIMARY KEY,
          name VARCHAR(100),
          value DOUBLE PRECISION,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // Insert some test data (using different approach)
      await conn.run(`
        INSERT INTO postgres_db.memory_test_table (name, value, id)
        SELECT 'test_' || i, random() * 1000, i
        FROM range(1000) t(i)
        WHERE NOT EXISTS (SELECT 1 FROM postgres_db.memory_test_table LIMIT 1);
      `);

      console.log('✅ PostgreSQL setup complete');
    } catch (error) {
      console.error('❌ PostgreSQL setup failed:', error.message);
      throw error;
    } finally {
      if (CONFIG.newConnectionPerQuery) {
        await conn.closeSync();
      }
    }
  }

  async setupSQLite() {
    console.log('📁 Setting up SQLite connection...');

    const conn = CONFIG.newConnectionPerQuery
      ? await duckdbInstance.connect()
      : connection;

    try {
      // Install and load sqlite extension
      await conn.run("INSTALL sqlite;");
      await conn.run("LOAD sqlite;");

      // Create SQLite file and attach
      await conn.run(`ATTACH '${CONFIG.sqliteFile}' AS sqlite_db (TYPE sqlite);`);

      // Create test table
      await conn.run(`
        CREATE TABLE IF NOT EXISTS sqlite_db.memory_test_table (
          id INTEGER PRIMARY KEY,
          name TEXT,
          value REAL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // Insert some test data (using INSERT OR REPLACE for SQLite compatibility)
      await conn.run(`
        INSERT INTO sqlite_db.memory_test_table (id, name, value)
        SELECT i, 'test_' || i, random() * 1000
        FROM range(1000) t(i)
        WHERE NOT EXISTS (SELECT 1 FROM sqlite_db.memory_test_table LIMIT 1);
      `);

      console.log('✅ SQLite setup complete');
    } catch (error) {
      console.error('❌ SQLite setup failed:', error.message);
      throw error;
    } finally {
      if (CONFIG.newConnectionPerQuery) {
        await conn.closeSync();
      }
    }
  }

  async setupDuckDBOnly() {
    console.log('🦆 Setting up DuckDB-only test...');

    const conn = CONFIG.newConnectionPerQuery
      ? await duckdbInstance.connect()
      : connection;

    try {
      // Create test table in DuckDB
      await conn.run(`
        CREATE TABLE IF NOT EXISTS memory_test_table (
          id INTEGER PRIMARY KEY,
          name VARCHAR(100),
          value DOUBLE,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // Insert test data (using different approach)
      await conn.run(`
        INSERT INTO memory_test_table (id, name, value)
        SELECT i, 'test_' || i, random() * 1000
        FROM range(1000) t(i)
        WHERE NOT EXISTS (SELECT 1 FROM memory_test_table LIMIT 1);
      `);

      console.log('✅ DuckDB-only setup complete');
    } catch (error) {
      console.error('❌ DuckDB-only setup failed:', error.message);
      throw error;
    } finally {
      if (CONFIG.newConnectionPerQuery) {
        await conn.closeSync();
      }
    }
  }

  getMemoryUsage() {
    const usage = process.memoryUsage();
    return {
      timestamp: Date.now(),
      iteration: iterationCount,
      rss: usage.rss,
      heapTotal: usage.heapTotal,
      heapUsed: usage.heapUsed,
      external: usage.external,
      arrayBuffers: usage.arrayBuffers
    };
  }

  logMemoryUsage() {
    const memory = this.getMemoryUsage();
    memoryStats.push(memory);

    // Only log to console every N iterations
    if (iterationCount % CONFIG.memoryLogInterval === 0 || iterationCount === 1) {
      const mb = (bytes) => (bytes / 1024 / 1024).toFixed(2);

      console.log(`📊 Memory [Iter ${iterationCount}] - RSS: ${mb(memory.rss)}MB, Heap: ${mb(memory.heapUsed)}/${mb(memory.heapTotal)}MB, External: ${mb(memory.external)}MB`);

      // Force garbage collection if enabled
      if (CONFIG.forceGC && global.gc) {
        global.gc();
        console.log('🗑️  Forced garbage collection');
      }
    }
  }

  async runQuery(mode) {
    const conn = CONFIG.newConnectionPerQuery
      ? await duckdbInstance.connect()
      : connection;

    try {
      let query;
      let tableName;

      switch (mode) {
        case 'duckdb':
          tableName = 'memory_test_table';
          break;
        case 'postgres':
          tableName = 'postgres_db.memory_test_table';
          break;
        case 'sqlite':
          tableName = 'sqlite_db.memory_test_table';
          break;
        default:
          throw new Error(`Unknown mode: ${mode}`);
      }

      // Complex query that should stress the system
      query = `
        WITH stats AS (
          SELECT
            COUNT(*) as total_rows,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value,
            STDDEV(value) as stddev_value
          FROM ${tableName}
        ),
        ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (ORDER BY value DESC) as rank
          FROM ${tableName}
        )
        SELECT
          s.total_rows,
          s.avg_value,
          s.min_value,
          s.max_value,
          s.stddev_value,
          COUNT(r.id) as top_100_count,
          AVG(r.value) as top_100_avg
        FROM stats s
        CROSS JOIN ranked r
        WHERE r.rank <= 100
        GROUP BY s.total_rows, s.avg_value, s.min_value, s.max_value, s.stddev_value;
      `;

      const startQueryTime = Date.now();
      const result = await conn.runAndReadAll(query);
      const queryTime = Date.now() - startQueryTime;

      const rows = result.getRows();

      // Only log query details every N iterations
      if (iterationCount % CONFIG.memoryLogInterval === 0 || iterationCount === 1) {
        console.log(`✅ ${mode.toUpperCase()} query completed in ${queryTime}ms, returned ${rows.length} rows`);
      }

      return { queryTime, rowCount: rows.length };
    } catch (error) {
      console.error(`❌ Error running ${mode} query:`, error.message);
      throw error;
    } finally {
      if (CONFIG.newConnectionPerQuery) {
        await conn.closeSync();
      }
    }
  }

  async runTest(mode) {
    console.log(`\n🚀 Starting memory leak test - Mode: ${mode.toUpperCase()}`);
    console.log(`📋 Config:`, {
      queryInterval: CONFIG.queryInterval,
      maxIterations: CONFIG.maxIterations || 'infinite',
      newConnectionPerQuery: CONFIG.newConnectionPerQuery,
      forceGC: CONFIG.forceGC
    });

    // Setup based on mode
    await this.setupDuckDB();

    switch (mode) {
      case 'postgres':
        await this.setupPostgreSQL();
        break;
      case 'sqlite':
        await this.setupSQLite();
        break;
      case 'duckdb':
        await this.setupDuckDBOnly();
        break;
    }

    // Initial memory reading
    this.logMemoryUsage();

    // Main test loop
    const queryTimer = setInterval(async () => {
      iterationCount++;

      try {
        // Log iteration progress less frequently
        if (iterationCount % CONFIG.memoryLogInterval === 0 || iterationCount === 1) {
          console.log(`\n🔄 Iteration ${iterationCount} (${mode.toUpperCase()})`);
        }

        await this.runQuery(mode);
        this.logMemoryUsage();

        if (CONFIG.maxIterations > 0 && iterationCount >= CONFIG.maxIterations) {
          console.log(`\n✅ Completed ${CONFIG.maxIterations} iterations`);
          clearInterval(queryTimer);
          this.cleanup();
          process.exit(0);
        }
      } catch (error) {
        console.error(`❌ Error in iteration ${iterationCount}:`, error.message);
      }
    }, CONFIG.queryInterval);

    console.log(`\n⏰ Running queries every ${CONFIG.queryInterval}ms...`);
    console.log('Press Ctrl+C to stop the test\n');
  }

  async runAllTests() {
    const modes = ['duckdb', 'sqlite', 'postgres'];

    for (const mode of modes) {
      console.log(`\n${'='.repeat(50)}`);
      console.log(`🧪 Testing mode: ${mode.toUpperCase()}`);
      console.log(`${'='.repeat(50)}`);

      try {
        // Reset counters
        iterationCount = 0;
        memoryStats.length = 0;
        startTime = Date.now();

        await this.runTest(mode);

        // Wait a bit between tests
        console.log('\n⏳ Waiting 10 seconds before next test...');
        await new Promise(resolve => setTimeout(resolve, 10000));

      } catch (error) {
        console.error(`❌ Test failed for mode ${mode}:`, error.message);
      }
    }
  }

  async start() {
    console.log('🧪 DuckDB Memory Leak Investigation Tool');
    console.log('=====================================\n');

    try {
      if (CONFIG.mode === 'all') {
        await this.runAllTests();
      } else {
        await this.runTest(CONFIG.mode);
      }
    } catch (error) {
      console.error('❌ Fatal error:', error.message);
      this.cleanup();
      process.exit(1);
    }
  }
}

// Main execution
if (import.meta.url === `file://${process.argv[1]}`) {
  const tester = new MemoryLeakTester();
  tester.start().catch(error => {
    console.error('❌ Unhandled error:', error);
    process.exit(1);
  });
}

export default MemoryLeakTester;