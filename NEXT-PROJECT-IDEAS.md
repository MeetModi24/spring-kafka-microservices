# Portfolio Project Ideas - AI + Distributed Systems

**Date:** 2026-07-10  
**Context:** After completing Kafka SAGA microservices learning project, researched portfolio-worthy projects combining Spring Boot + Kafka + AI/ML + Frontend + Deployment

**Research conducted by:** 3 specialized agents (RAG systems, ML+Kafka platforms, Deployment patterns)

---

## 🎯 Project Selection Criteria

Based on requirements analysis:
- ✅ **Spring Boot + Java** backend
- ✅ **React/Vue/Next.js** frontend (full-stack, visual demo)
- ✅ **AI/ML integration** (LLMs, RAG, vector search - 2026 relevant)
- ✅ **Distributed systems** (Kafka, event-driven, microservices)
- ✅ **Deployable** (live URL, not just localhost)
- ✅ **Unique** (not generic e-commerce CRUD)
- ✅ **Reference code available** (avoid implementation hazards)
- ✅ **Learning value** (increases technical depth)

---

## 🥇 PROJECT 1: EventRAG - Event-Driven RAG Platform ⭐ TOP PICK

### Overview
Multi-tenant document knowledge base with real-time ingestion via Kafka, RAG queries, and live frontend. Combines event-driven microservices with modern RAG patterns.

### Why Portfolio-Worthy
- **Ecosystem gap:** Spring Boot + Kafka + RAG + Frontend + Deployed barely exists on GitHub
- **2026 relevance:** RAG is the hottest enterprise AI pattern
- **Leverages existing expertise:** Builds on Kafka microservices knowledge
- **Full-stack:** Next.js frontend, Spring Boot backend, vector database
- **Distributed systems:** Event-driven pipeline, async processing, microservices
- **Deployable:** Clear path to $0/month hosting

### Architecture
```
Frontend (Next.js + React)
    ↓ REST + WebSocket
API Gateway (Spring Boot)
    ↓ Kafka
Document Processor (Spring Boot) → Embedding Service (Spring Boot + Spring AI)
    ↓
PostgreSQL + pgvector (Supabase)
    ↓
RAG Query Service (Spring Boot + Spring AI)
```

### Microservices
1. **API Gateway** - Upload endpoints, query endpoints, WebSocket for status
2. **Document Processor** - Text extraction (PDF/DOCX), chunking, metadata
3. **Embedding Service** - Generate embeddings, store in pgvector
4. **RAG Query Service** - Semantic search, context retrieval, LLM generation

### Tech Stack
- **Backend:** Spring Boot 3.3+, Spring AI 1.0+, Java 21
- **Frontend:** Next.js 15, React 19, shadcn/ui, Tailwind CSS
- **Messaging:** Kafka (Upstash or Redpanda Cloud)
- **Vector DB:** PostgreSQL + pgvector (Supabase)
- **AI:** OpenAI API (embeddings + GPT-4o-mini) or Groq (free)
- **Deployment:** Fly.io (backend) + Vercel (frontend)

### Kafka Topics
- `document-uploaded` - Triggers processing pipeline
- `document-chunked` - After text extraction and chunking
- `embedding-completed` - After vector storage
- `processing-status` - For WebSocket updates to frontend

### Key Features
- Document upload (PDF, DOCX, TXT)
- Real-time processing status (WebSocket)
- Chat interface with streaming LLM responses
- Semantic search across documents
- Multi-tenant support
- Document library with metadata

### Distributed Systems Patterns
- Event-driven architecture
- CQRS (write = upload, read = query)
- Async processing pipeline
- Microservices communication
- Eventual consistency

### Timeline
- **Week 1:** Core RAG locally (upload → embed → query)
- **Week 2:** Event-driven pipeline with Kafka
- **Week 3:** Frontend polish + streaming responses
- **Week 4:** Deployment + documentation

### Cost
- **Total:** $0/month + ~$1 in LLM API usage for demos
- Frontend (Vercel): Free
- Backend 3 services (Fly.io): Free (256MB VMs)
- Database (Supabase): Free (500MB + pgvector)
- Kafka (Upstash): Free (10k msgs/day)
- LLM API (OpenAI): Pay-as-you-go

### Reference Projects
1. **langchain4j-aideepin** (1.3k ⭐) - Full-stack RAG, Vue3 frontend
2. **piomin/spring-ai-showcase** (300 ⭐) - Spring AI RAG patterns
3. **spring-petclinic-ai** (400 ⭐) - Spring AI chatbot, Docker deployment
4. **HemantMedhsia/RAG** - Spring Boot + React + pgvector
5. **ThomasVitale/llm-apps-java-spring-ai** (757 ⭐) - Clean examples

### Learning Outcomes
- RAG architecture and implementation
- Vector embeddings and semantic search
- Event-driven microservices with Kafka
- Spring AI framework
- Next.js full-stack development
- PaaS deployment (Fly.io, Vercel)
- WebSocket real-time communication

### Portfolio Value
**VERY HIGH** - Fills ecosystem gap, modern AI, distributed systems, full-stack, deployed

---

## 🥈 PROJECT 2: StreamGuard - Real-Time AI Fraud Detection

### Overview
Event-driven fraud detection system for financial transactions using Kafka Streams + AI scoring with live monitoring dashboard.

### Why Portfolio-Worthy
- Builds on existing order/payment SAGA project
- Adds AI/ML layer (fraud scoring via LLM or ML model)
- Real-time streaming with Kafka Streams
- Live WebSocket dashboard showing detections
- Directly relevant to fintech/payments industry

### Architecture
```
Frontend (React Dashboard)
    ↓
API Gateway (Spring Boot)
    ↓ Kafka
Transaction Stream → Fraud Detector (Spring AI / ML Model)
                           ↓
                    Kafka Streams Aggregator
                    (velocity rules, patterns)
                           ↓
                    Alert Service → Dashboard (WebSocket)
```

### Core Components
1. **Transaction Generator** - Simulates payment transactions
2. **Fraud Detector** - AI scoring (LLM-based or ML model via TensorFlow Serving)
3. **Velocity Analyzer** - Kafka Streams for N txns/minute rules
4. **Alert Service** - Real-time notifications
5. **Dashboard** - Live visualization with charts

### Tech Stack
- **Base:** Existing `spring-kafka-microservices` codebase
- **Add:** Spring AI for fraud scoring
- **Add:** Kafka Streams for aggregations
- **Add:** React + Recharts for dashboard
- **Add:** WebSocket for live updates
- **ML Option:** TensorFlow Serving (gRPC) or embedded model

### Fraud Detection Patterns
- **Velocity rules:** N transactions in M minutes from same IP/card
- **AI scoring:** LLM analyzes transaction context for anomalies
- **Pattern detection:** Session windows for coordinated attacks
- **Geographic:** Impossible travel (two locations, short time)

### Kafka Streams Features
- Windowed aggregations (tumbling, session windows)
- Stateful processing with RocksDB
- Interactive queries (query fraud state in real-time)
- Exactly-once semantics

### Reference Projects
1. **Ramanjaneyareddy/payments-flow** - Spring Boot 3, real-time fraud detection
2. **plsyz/finstream-ai** - Multi-agent fraud detection, pgvector + Neo4j
3. **kaiwaehner/kafka-streams-machine-learning-examples** (911 ⭐) - ML in Kafka Streams
4. **altayyeles/Real-Time-Fraud-Detection-Kafka-AI** - Live dashboard UI patterns

### Timeline
**2-3 weeks** (leverages existing codebase)

### Cost
Similar to EventRAG (~$0/month + API costs)

### Learning Outcomes
- Real-time stream processing with Kafka Streams
- ML model integration (serving patterns)
- Windowed aggregations and stateful processing
- Interactive queries
- Real-time dashboard with WebSocket

### Portfolio Value
**HIGH** - Evolution of existing project, fintech relevance, real-time ML

---

## 🥉 PROJECT 3: AgentoFlow - Multi-Agent Orchestration Platform

### Overview
Platform for orchestrating multiple AI agents using Kafka as the inter-agent message bus, with visual workflow builder UI.

### Why Portfolio-Worthy
- Multi-agent systems are 2026's hottest AI topic
- Uses Kafka as agent communication layer (distributed, persistent)
- Visual workflow builder (like Temporal/n8n but AI-native)
- Demonstrates advanced orchestration patterns
- Rare in Java ecosystem

### Architecture
```
Frontend (React Flow - visual workflow builder)
    ↓
API Gateway (Spring Boot)
    ↓ Kafka (agent-messages topic)
Agent Pool (Spring AI agents):
  - RAG Agent (document search)
  - Code Agent (code generation/review)
  - Search Agent (web search)
  - Data Agent (SQL queries)
    ↓
Workflow Orchestrator (Spring Boot + LangGraph4j)
    ↓
Result Aggregator
```

### Core Concepts
- **Agent Types:** Different specialized AI agents with distinct capabilities
- **Kafka Message Bus:** Agents communicate via Kafka topics
- **Workflow DSL:** YAML or visual definition of agent sequences
- **State Management:** Persistent workflow state in Kafka/DB
- **Inter-agent Communication:** Pub/sub for agent collaboration

### Tech Stack
- **Backend:** Spring Boot + Spring AI + LangGraph4j
- **Frontend:** React Flow (visual workflow designer)
- **Messaging:** Kafka for agent-to-agent messages
- **Orchestration:** LangGraph4j for agent workflows
- **Agents:** Multiple Spring AI agents with different tools

### Agent Examples
1. **RAG Agent** - Search knowledge base, retrieve context
2. **Code Agent** - Generate code, review code, explain code
3. **Search Agent** - Web search, summarize results
4. **SQL Agent** - Text-to-SQL, query databases
5. **Orchestrator Agent** - Coordinates other agents

### Kafka Topics
- `agent-tasks` - New tasks for agents
- `agent-responses` - Agent outputs
- `workflow-state` - Workflow execution state
- `agent-coordination` - Inter-agent coordination messages

### Visual Workflow Builder
- Drag-and-drop agent nodes
- Connect agents with arrows (data flow)
- Configure agent parameters
- Save/load workflows
- Execute and monitor

### Reference Projects
1. **mouse1999/nexus-agentic-commerce** - Agent orchestration with Spring AI
2. **langgraph4j/langgraph4j** (1.8k ⭐) - Agent workflow framework in Java
3. **cool-icu0/xgent** (113 ⭐) - Spring AI + LangChain4j multi-agent + MCP
4. **mateclaw** (748 ⭐) - Spring AI multi-agent orchestration

### Timeline
**4-6 weeks** (most ambitious)

### Learning Outcomes
- Multi-agent system architecture
- Agent orchestration patterns
- LangGraph4j framework
- Visual workflow design
- Complex event-driven coordination

### Portfolio Value
**VERY HIGH** - Cutting-edge, rare in Java, impressive complexity

---

## 📊 Project Comparison

| Aspect | EventRAG | StreamGuard | AgentoFlow |
|--------|----------|-------------|------------|
| **Complexity** | Medium | Medium | High |
| **Timeline** | 3-4 weeks | 2-3 weeks | 4-6 weeks |
| **AI Relevance** | Very High (RAG) | High (Real-time ML) | Very High (Agents) |
| **Kafka Usage** | Heavy | Heavy | Heavy |
| **Frontend** | Next.js chat | React dashboard | React Flow |
| **Cost** | $0-1/mo | $0-1/mo | $0-5/mo |
| **Reference Code** | Excellent | Good | Good |
| **Uniqueness** | High | Medium | Very High |
| **Deployability** | Excellent | Good | Good |
| **Interview Impact** | Very High | High | Very High |
| **Learning Curve** | Medium | Low-Medium | High |

---

## 🎖️ Recommendation Priority

### 1st Choice: **EventRAG** ⭐⭐⭐⭐⭐
**Why:** Best balance of novelty, feasibility, reference code, and portfolio impact. Fills clear ecosystem gap. Leverages existing Kafka expertise while adding modern AI. Excellent deployment story.

**Best for:** Demonstrating distributed systems + AI integration + full-stack skills

### 2nd Choice: **StreamGuard** ⭐⭐⭐⭐
**Why:** Fastest path by extending existing project. Strong industry relevance (fintech). Real-time streaming is impressive. Good learning curve.

**Best for:** Quick win, building on existing work, fintech focus

### 3rd Choice: **AgentoFlow** ⭐⭐⭐⭐⭐
**Why:** Most ambitious and cutting-edge. Multi-agent systems are hot. Visual workflow builder is impressive. Higher risk/reward.

**Best for:** Maximum "wow" factor, longer timeline acceptable, interest in AI agents

---

## 💡 Hybrid Options

### Option A: Start with EventRAG, Add StreamGuard Features
1. Build EventRAG core (4 weeks)
2. Add fraud detection agent to RAG system (1 week)
3. Demonstrates both RAG and real-time ML

### Option B: EventRAG with Agent Features
1. Build EventRAG (4 weeks)
2. Add multiple specialized agents (RAG agent, summarization agent, etc.)
3. Show agent orchestration in RAG context

### Option C: Phased Development
**Phase 1:** Build EventRAG with simple UI (2 weeks)
**Phase 2:** Add Kafka event pipeline (2 weeks)
**Phase 3:** Add multi-agent features (2 weeks)
**Phase 4:** Add fraud detection/monitoring (2 weeks)

Result: Comprehensive platform over 8 weeks

---

## 🎯 Decision Framework

**Choose EventRAG if:**
- Want to learn RAG and vector databases
- Interested in modern AI patterns
- Want strong reference code
- Need fast path to deployed demo
- Prefer balanced complexity

**Choose StreamGuard if:**
- Want to extend existing project
- Interested in real-time ML
- Prefer shorter timeline
- Focusing on fintech/payments
- Like data visualization

**Choose AgentoFlow if:**
- Fascinated by multi-agent systems
- Want cutting-edge resume project
- Can invest 4-6 weeks
- Enjoy complex architectures
- Want maximum differentiation

---

## 📚 Next Steps

1. **Review all reference projects** listed in REFERENCE-PROJECTS.md
2. **Choose project** based on interests and timeline
3. **Create new project folder** for chosen project
4. **Start with ARCHITECTURE.md** before any coding
5. **Set up reference projects locally** for study
6. **Follow week-by-week implementation plan**
7. **Document as you build** (same pattern as current project)
8. **Deploy early and often** (test deployment pipeline)

---

## 📖 Additional Resources

See companion files:
- **REFERENCE-PROJECTS.md** - All 30+ GitHub projects found by research
- **DEPLOYMENT-GUIDE.md** - Detailed deployment strategies and costs
- **TECH-STACK-DETAILS.md** - Deep dive on Spring AI, pgvector, etc.

---

**Status:** Research completed 2026-07-10  
**Next:** Choose project and create new folder to start planning

---

## 🏆 PROJECT 4: QuantStream - Real-Time Trading Strategy Analytics Platform ⭐ HIGH SCALE

**Research Date:** 2026-07-11  
**Research Source:** 3 specialized agents (Trading dashboards, Time-series + Kafka, Data simulation)

### Problem Statement (Your Requirement)

Design and build a modern, user-friendly data dashboard for Operations teams and Quantitative analysts to monitor, analyze, and visualize trading strategies across different environments.

**System Context:**
- **Strategy Layer:** Generates token-level data via messages
- **Data Aggregator:** Consolidates data from multiple strategies
- **Scale:** 30,000 tokens × 1 msg/sec × 30 strategies = **~900,000 messages/second peak**
- **Requirements:** Web-based, real-time streaming, intraday analytics, authentication, chart visualization

### Why Portfolio-Worthy

**This is ENTERPRISE-GRADE scale** - significantly higher than typical portfolio projects:

- ✅ **Ecosystem gap:** "Very few open-source projects are true end-to-end Spring Boot + Kafka + TSDB + dashboard at 900K scale" (per research)
- ✅ **Real-world problem:** Actual trading firm use case with concrete requirements
- ✅ **High throughput:** 900K msg/sec requires partitioning, parallel processing, aggregation
- ✅ **Distributed systems:** Kafka Streams, time-series DB, WebSocket streaming
- ✅ **Full-stack:** React dashboard with real-time charts
- ✅ **Data engineering:** Time-series storage, retention policies, aggregations
- ✅ **Unique scope:** No reference project does this exact combination

**Interview Impact:** Can say "I built a system handling 900K msg/sec with sub-second dashboard latency"

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  FRONTEND (React + Lightweight Charts / TradingView)        │
│                                                              │
│  - Strategy comparison dashboard                            │
│  - Real-time candlestick charts (1s/1m/5m aggregations)    │
│  - Strategy leaderboard (PnL, volume, win rate)            │
│  - Token watchlist with live updates                        │
│  - WebSocket subscription management                        │
│                                                              │
│  Deploy: Vercel                                             │
└─────────────────────────────────────────────────────────────┘
                         ↓ WebSocket (STOMP)
┌─────────────────────────────────────────────────────────────┐
│  API GATEWAY + WEBSOCKET SERVICE (Spring Boot)              │
│                                                              │
│  - REST API: Historical queries, strategy metadata          │
│  - WebSocket: Real-time updates (1-10 msg/sec/client)      │
│  - Authentication: Spring Security + JWT                    │
│  - Per-client subscriptions (don't stream all 30K tokens)  │
│                                                              │
│  Deploy: Fly.io                                             │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  DATA GENERATOR SERVICE (Spring Boot)                       │
│                                                              │
│  - Geometric Brownian Motion (GBM) price generator          │
│  - 30,000 tokens with tiered volatility                     │
│  - Correlation matrix (sector co-movement)                  │
│  - Batched Kafka producer (linger.ms=5, compression=lz4)   │
│  - ~300 LOC, handles 900K msg/sec on single node           │
│                                                              │
│  Deploy: Fly.io                                             │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  KAFKA (Upstash or Redpanda Cloud)                          │
│                                                              │
│  Topics (30-60 partitions each):                            │
│  - market-data (30K tokens × 1 msg/sec = 30K msg/sec)      │
│  - strategy-signals (30 strategies × trades = 1-5K msg/sec) │
│  - aggregated-candles (server-side 1s/1m/5m aggregations)  │
│                                                              │
│  Partitioning: By token symbol (30K keys distribute well)  │
└─────────────────────────────────────────────────────────────┘
        ↓                                          ↓
┌───────────────────────────┐    ┌────────────────────────────┐
│  KAFKA STREAMS AGGREGATOR │    │  STRATEGY EVALUATOR        │
│  (Spring Boot)            │    │  (Spring Boot × 30)        │
│                           │    │                            │
│  - Windowed aggregations  │    │  - One instance per        │
│    (tumbling 1s/1m/5m)    │    │    strategy                │
│  - OHLC candle generation │    │  - Technical indicators    │
│  - Volume calculations    │    │    (MA, RSI, momentum)     │
│  - Publish to             │    │  - Trade signals           │
│    aggregated-candles     │    │  - Publish to              │
│                           │    │    strategy-signals        │
│  Partitions: 3-6 instances│    │                            │
│  (200-300K msg/sec each)  │    │  Deploy: Fly.io (×30)      │
│                           │    │                            │
│  Deploy: Fly.io           │    └────────────────────────────┘
└───────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  TIME-SERIES DATABASE                                        │
│                                                              │
│  QuestDB (RECOMMENDED) or ClickHouse                        │
│  - 1.6M rows/sec ingestion (QuestDB benchmark)              │
│  - SQL queries with time-based indexing                     │
│  - Retention tiers:                                         │
│    * Hot (5 min): Raw ticks in-memory                       │
│    * Warm (30-90 days): 1s resolution on disk              │
│    * Cold (1 year): Downsampled to 1m, S3 Parquet          │
│                                                              │
│  NOT TimescaleDB (caps at 200-400K/sec, insufficient)      │
│                                                              │
│  Deploy: Self-hosted on Fly.io or QuestDB Cloud            │
└─────────────────────────────────────────────────────────────┘
```

### Microservices (6 services)

1. **Data Generator Service**
   - Generates realistic market data using GBM
   - 30K tokens with tiered volatility
   - Produces to `market-data` topic

2. **Strategy Evaluator Services (×30)**
   - One instance per strategy
   - Consumes market data
   - Applies trading rules (MA cross, RSI, etc.)
   - Produces trade signals

3. **Kafka Streams Aggregator**
   - Windowed aggregations (1s/1m/5m candles)
   - OHLC calculations
   - Volume aggregations
   - 3-6 instances for 900K msg/sec

4. **API Gateway Service**
   - REST API for historical queries
   - Authentication and authorization
   - Rate limiting per user

5. **WebSocket Service**
   - Real-time streaming to dashboard
   - Per-client subscriptions
   - Aggregates updates (1-10 msg/sec to browser)

6. **Analytics Service**
   - Strategy performance calculations
   - PnL tracking
   - Risk metrics

### Tech Stack

**Backend:**
- **Framework:** Spring Boot 3.3+, Java 21
- **Messaging:** Kafka (Upstash or Redpanda Cloud)
- **Stream Processing:** Kafka Streams
- **Time-series DB:** QuestDB (1.6M rows/sec) or ClickHouse
- **WebSocket:** Spring WebSocket (STOMP protocol)
- **Authentication:** Spring Security + JWT
- **Dependencies:**
  - Spring Kafka
  - Spring WebSocket
  - Spring Data JPA
  - QuestDB JDBC driver
  - ta4j (technical analysis library)

**Frontend:**
- **Framework:** React 18 + TypeScript
- **Charts:** Lightweight Charts (TradingView OSS) or Apache ECharts
- **Real-time:** STOMP client over WebSocket
- **UI:** Tailwind CSS + shadcn/ui
- **State:** Zustand or Redux Toolkit
- **Features:**
  - Multi-strategy comparison
  - Real-time candlestick charts
  - Strategy leaderboard
  - Token watchlist
  - Intraday analytics

**Data Generation:**
- **Approach:** Geometric Brownian Motion (GBM)
- **Model:** `dS = μS dt + σS dW` (Black-Scholes foundation)
- **Realism:** Tiered volatility + correlation matrix
- **Performance:** 900K msg/sec on single node
- **Seeding:** CoinGecko API for real token list

**Deployment:**
- **Frontend:** Vercel (free)
- **Backend Services:** Fly.io (3-6 instances)
- **QuestDB:** Self-hosted on Fly.io or QuestDB Cloud
- **Kafka:** Upstash (free: 10K msgs/day - need paid) or Redpanda Cloud

### Kafka Topics

```
market-data (30-60 partitions)
├─ Key: token symbol (30K keys)
├─ Value: {symbol, price, volume, timestamp}
├─ Rate: 30K msg/sec
└─ Retention: 5 minutes (hot tier)

strategy-signals (10 partitions)
├─ Key: strategy ID
├─ Value: {strategyId, tokenId, action, confidence, timestamp}
├─ Rate: 1-5K msg/sec
└─ Retention: 24 hours

aggregated-candles (30 partitions)
├─ Key: token symbol
├─ Value: {symbol, open, high, low, close, volume, interval, timestamp}
├─ Rate: 30K msg/sec (1s candles) + 500 msg/sec (1m) + 100 msg/sec (5m)
└─ Retention: 7 days
```

### Key Features

**Real-Time Dashboard:**
- Live candlestick charts (1s/1m/5m/15m intervals)
- Multi-strategy overlay comparison
- Strategy leaderboard (sorted by PnL, win rate, Sharpe ratio)
- Token watchlist with price alerts
- WebSocket streaming with auto-reconnect

**Intraday Analytics:**
- Current day PnL per strategy
- Volume profile by hour
- Top gainers/losers
- Correlation heatmap
- Risk metrics (volatility, max drawdown)

**Strategy Monitoring:**
- Active strategies health check
- Trade execution timeline
- Signal distribution histogram
- Strategy-specific performance metrics

**Authentication:**
- Role-based access (Ops team vs Quant analysts)
- Different views per role
- API rate limiting
- Session management

### Distributed Systems Patterns

**High Throughput:**
- Kafka partitioning (30-60 partitions)
- Parallel Kafka Streams instances (3-6 nodes)
- Batched producer (linger.ms=5, compression=lz4)
- Single Spring Boot ceiling: ~200-300K msg/sec

**Aggregation:**
- Server-side windowing (Kafka Streams tumbling windows)
- **Never stream raw ticks to browser** (900K → 1-10 msg/sec)
- Multiple time resolutions (1s/1m/5m/15m candles)

**Time-Series Storage:**
- QuestDB for hot/warm queries (SQL + nanosecond timestamps)
- Retention tiers: Hot (in-memory 5 min) → Warm (disk 90 days) → Cold (S3 Parquet)
- Partitioned by date for efficient queries

**WebSocket Streaming:**
- Per-client subscriptions (subscribe to specific tokens/strategies)
- Throttling (max 10 updates/sec per client)
- STOMP protocol over WebSocket
- Heartbeat for connection health

### Data Generation Strategy

**Token Universe (30,000 tokens):**
1. Pull top 500 from CoinGecko API (real tokens, one-time seed)
2. Generate 29,500 synthetic tokens with tiered volatility:
   - **Tier 1 (Majors, 100):** σ=0.02/day, drift=0 (BTC, ETH style)
   - **Tier 2 (Mid-cap, 1,000):** σ=0.05/day (altcoins)
   - **Tier 3 (Long-tail, 28,900):** σ=0.15/day, jump-diffusion (meme coins)

**GBM Generator (~300 LOC):**
```java
@Scheduled(fixedRate = 1000)  // 1 sec tick
void generateTick() {
    tokens.parallelStream().forEach(token -> {
        double dt = 1.0 / 86400;  // day fraction
        double dW = random.nextGaussian() * Math.sqrt(dt);
        
        // GBM: dS = μS dt + σS dW
        token.price *= Math.exp(
            (drift - 0.5 * vol * vol) * dt + vol * dW
        );
        
        producer.send("market-data", token.symbol, 
            new Tick(token.symbol, token.price, volume, timestamp)
        );
    });
}
```

**Correlation Matrix:**
- Cluster tokens into 5 sectors that co-move
- Apply correlation factor to make price movements realistic

**Strategy Signal Generation:**
- Option A: Real strategies consume market data, emit signals (realistic)
- Option B: Stochastic signal generation (simpler for demo)

### Timeline

**8-10 weeks for complete system**

#### **Week 1-2: Data Pipeline Foundation**
- Set up Kafka (Upstash or local)
- Implement GBM data generator
- Kafka Streams aggregator (1s/1m candles)
- Basic QuestDB schema and ingestion

#### **Week 3-4: Backend Services**
- API Gateway with REST endpoints
- WebSocket service with STOMP
- Strategy evaluator template (×1 instance)
- Authentication (Spring Security + JWT)

#### **Week 5-6: Frontend Dashboard**
- React app setup with TypeScript
- Lightweight Charts integration
- WebSocket client (STOMP)
- Real-time candlestick charts
- Strategy leaderboard UI

#### **Week 7: Strategy Implementation**
- Deploy 5-10 strategy instances (MA cross, RSI, momentum)
- Strategy performance tracking
- Trade signal visualization

#### **Week 8: Analytics & Polish**
- Intraday analytics (PnL, volume, risk metrics)
- Multi-strategy comparison views
- Chart annotations and alerts
- Error handling and reconnection

#### **Week 9: Scaling & Optimization**
- Load testing (validate 900K msg/sec)
- Add Kafka Streams instances for parallelism
- Optimize WebSocket throttling
- QuestDB query optimization

#### **Week 10: Deployment & Documentation**
- Deploy to Fly.io (multiple services)
- QuestDB Cloud or self-hosted
- ARCHITECTURE.md, DEPLOYMENT.md, README.md
- Demo video with live URL

### Cost Analysis

**Development (Local):**
- Kafka: Docker Compose (free)
- QuestDB: Docker (free)
- Services: Local Spring Boot (free)

**Production (Deployed):**

| Component | Platform | Resources | Monthly Cost |
|-----------|----------|-----------|--------------|
| Frontend | Vercel | Unlimited hobby | $0 |
| API Gateway | Fly.io | 256MB VM | $0 |
| WebSocket Service | Fly.io | 512MB VM | $5-10 |
| Data Generator | Fly.io | 256MB VM | $0 |
| Kafka Streams (×3) | Fly.io | 256MB VMs | $0 (free tier) |
| Strategy Evaluators (×5) | Fly.io | 256MB VMs | $0-10 |
| **Kafka** | Upstash Pro | 100K msgs/day | **$30-50** |
| **QuestDB** | Self-hosted Fly.io | 2GB VM | **$20-30** |
| **Total** | | | **$55-100/month** |

**Notes:**
- 900K msg/sec exceeds Upstash free tier (10K msgs/day)
- Could use Redpanda Cloud (similar pricing)
- QuestDB Cloud is more expensive (~$100+/month)
- Self-hosted QuestDB on Fly.io is most cost-effective

**Cost-Saving Alternatives:**
- Reduce to 3,000 tokens (90K msg/sec) → Upstash free tier
- Run everything locally, record demo video, deploy static demo
- Use TimescaleDB on Supabase (lower scale but $0)

### Reference Projects

**Time-Series + Kafka (12 projects):**
1. **questdb/questdb** (17.1k ⭐) - Time-series DB, 1.6M rows/sec
2. **apache/pinot** (5.7k ⭐) - Real-time OLAP, 1M+ events/sec at LinkedIn
3. **ClickHouse + kafka-connect-clickhouse** - 1M+ rows/sec
4. **confluentinc/kafka-streams-examples** (2.7k ⭐) - Windowed aggregations
5. **apache/druid** - Millions/sec at Netflix scale
6. **timescale/tsbs** - Benchmark suite (TimescaleDB caps at 400K/sec)
7. **eclipse-hono/hono** (500 ⭐) - IoT, 250K msg/sec per node
8. **hivemq/hivemq-mqtt-kafka-demo** - MQTT→Kafka→InfluxDB→Grafana
9. **finos/traderx** (200 ⭐) - FINOS trading platform (Spring Boot + Kafka + React)
10. **saubury/kafka-streams-stockstats** - Tumbling windows for stocks
11. **spring-kafka samples** - Batch listener 200-500K/sec
12. **questdb/time-series-streaming-analytics-template** - QuestDB + Kafka demo

**Trading Platforms (19 projects):**
1. **StockSharp/StockSharp** (10.3k ⭐, C#) - Full algo platform, strategy monitoring
2. **TradeMaster-NTU/TradeMaster** (2.9k ⭐, Python) - Multi-strategy comparison
3. **OpenAlgo** (2.2k ⭐, Python) - Algo trading, Flask dashboards
4. **knowm/XChange** (4.1k ⭐, Java) - Unified API for 60+ exchanges
5. **ccxt/ccxt** (43.3k ⭐) - Market data connector library
6. **cassandre-tech/cassandre-trading-bot** (656 ⭐, Java) - **Spring Boot starter**
7. **ta4j/ta4j** (2.5k ⭐, Java) - Technical analysis library
8. **cointrader** (452 ⭐, Java) - CEP + backtesting
9. **univocity-trader** (599 ⭐, Java) - Multi-symbol support
10. **CoinExchange_CryptoExchange_Java** (1.7k ⭐, Java) - Spring Cloud exchange
11. **redtorch** (799 ⭐, Kotlin/Java) - Multi-strategy framework
12. **bxbot** (862 ⭐, Java) - Bitcoin trading bot
13. **YungHuang85/real-time-market-data-platform-backend** - Spring Boot + Kafka + React
14. **UtkarshAlshi/tradewise-monorepo** - Spring Boot + Kafka + Next.js
15. **Ra9huvansh/Valoris-Systems** - Trade lifecycle simulator
16. **prathamkr01/quant-trade-sim-platform** - HFT simulation
17. **ayushx007/java-hft-engine** - Java 21 + Spring Boot order matching
18. **JOravetz/stock-market-dashboard** - FastAPI + React + WebSocket patterns
19. **mplfinance** (4.4k ⭐, Python) - Financial charting

**Data Generation (8 approaches):**
1. **Custom GBM Generator** (recommended) - 300 LOC Spring Boot
2. **kafka-connect-datagen** (Confluent) - Not realistic for trading
3. **Binance data.binance.vision** - Historical CSV downloads
4. **CoinGecko API** - 10K+ tokens, seed volatility
5. **XChange library** - Real exchange data
6. **JQuantLib** - Quant models (overkill)
7. **Flink StockPrice example** - Uses GBM
8. **QuestDB time-series-streaming-analytics-template** - Tick generation patterns

### Learning Outcomes

**Distributed Systems:**
- High-throughput Kafka (30-60 partitions, 900K msg/sec)
- Kafka Streams windowed aggregations
- Parallel processing (3-6 Kafka Streams instances)
- Time-series data architecture
- WebSocket streaming with throttling
- Retention tiers (hot/warm/cold)

**Data Engineering:**
- Time-series database (QuestDB or ClickHouse)
- Data aggregation strategies (tumbling windows)
- Real-time analytics on streaming data
- Query optimization for time-series
- Data retention policies

**Full-Stack Development:**
- Spring Boot WebSocket (STOMP)
- React real-time updates
- Financial chart libraries (Lightweight Charts)
- Authentication and role-based access
- Multi-service coordination

**Financial Domain:**
- Trading strategy lifecycle
- Technical indicators (MA, RSI, momentum)
- OHLC candle generation
- PnL calculations
- Risk metrics (volatility, drawdown, Sharpe ratio)

**System Design:**
- Handling 900K msg/sec throughput
- Sub-second latency requirements
- Scalability patterns (horizontal scaling)
- Monitoring and observability
- Performance optimization

### Portfolio Value

**VERY HIGH - Enterprise Scale**

**Why Impressive:**
- ✅ **900K msg/sec scale** - Orders of magnitude above typical projects
- ✅ **Real-world problem** - Actual trading firm requirements
- ✅ **Ecosystem gap** - No reference project does this combination
- ✅ **Full-stack** - Backend + Frontend + Data + Deployment
- ✅ **Distributed systems depth** - Kafka Streams, partitioning, aggregation
- ✅ **Data engineering** - Time-series, retention, query optimization

**Interview Story:**
> "I built a real-time trading strategy analytics platform handling 900,000 messages per second across 30,000 tokens. The system uses Kafka with 30-60 partitions feeding into parallel Kafka Streams instances for windowed aggregations. Data flows to QuestDB (a high-performance time-series database) and streams to a React dashboard via WebSocket with per-client subscriptions. I implemented Geometric Brownian Motion for realistic market data simulation and deployed it as 6 Spring Boot microservices on Fly.io. The dashboard provides sub-second latency for 30 concurrent strategies with real-time candlestick charts and performance analytics."

**Unique Aspects:**
- Demonstrates handling enterprise-grade throughput
- Combines financial domain knowledge with engineering
- Shows end-to-end system design thinking
- Proves ability to work with time-series data at scale
- Real-world applicability (trading firms, fintech)

### Comparison to Other Projects

| Aspect | EventRAG | StreamGuard | AgentoFlow | **QuantStream** |
|--------|----------|-------------|------------|----------------|
| **Complexity** | Medium | Medium | High | **Very High** |
| **Scale** | Medium | Medium | Medium | **Enterprise** |
| **Timeline** | 3-4 weeks | 2-3 weeks | 4-6 weeks | **8-10 weeks** |
| **Throughput** | ~1K msg/sec | ~100K msg/sec | ~10K msg/sec | **900K msg/sec** |
| **Cost** | $0-1/mo | $0-1/mo | $0-5/mo | **$55-100/mo** |
| **AI Relevance** | Very High | High | Very High | Low |
| **Kafka Usage** | Heavy | Heavy | Heavy | **Extreme** |
| **Data Engineering** | Low | Medium | Low | **Very High** |
| **Frontend** | Chat UI | Dashboard | Visual builder | **Real-time charts** |
| **Reference Code** | Excellent | Good | Good | **Fragmented** |
| **Uniqueness** | High | Medium | Very High | **Extreme** |
| **Learning Curve** | Medium | Low-Medium | High | **Very High** |
| **Interview Impact** | Very High | High | Very High | **Exceptional** |

### Challenges & Considerations

**Technical Challenges:**
1. **Scale:** 900K msg/sec is non-trivial, requires careful partitioning
2. **Cost:** Production deployment exceeds free tiers (~$60-100/month)
3. **Complexity:** 6 microservices + Kafka + QuestDB + frontend
4. **No single reference:** Must compose patterns from multiple sources
5. **Data simulation:** Realistic trading data requires financial modeling

**Mitigation Strategies:**
1. **Scale down for demo:** Use 3,000 tokens (90K msg/sec) to fit free tier
2. **Local development:** Run everything in Docker Compose (free)
3. **Record demo:** Deploy, record video, then tear down (save costs)
4. **Phased approach:** Start with 1 strategy, 1K tokens, scale up gradually
5. **Reference projects:** Study QuestDB benchmarks, Kafka Streams examples

**When to Choose This Project:**
- Want to demonstrate enterprise-grade engineering
- Interested in financial/trading domain
- Comfortable with high complexity
- Have 8-10 weeks available
- Want to learn data engineering + distributed systems
- Willing to pay $60-100/month for deployment OR run locally with demo video

**When NOT to Choose:**
- Need fast portfolio piece (2-3 weeks)
- Limited distributed systems experience
- Want to focus on AI/ML features
- Prefer $0/month deployment
- Looking for abundant reference code

### Next Steps to Start

1. **Validate scale locally:**
   - Set up Kafka + QuestDB in Docker Compose
   - Implement GBM generator for 1,000 tokens
   - Test ingestion at 10K msg/sec
   - Measure QuestDB query latency

2. **Prototype dashboard:**
   - React + Lightweight Charts
   - WebSocket connection
   - Real-time candlestick updates
   - Prove concept works

3. **Study references:**
   - Clone QuestDB repo, read benchmarks
   - Study confluent Kafka Streams examples
   - Review cassandre-trading-bot for strategy patterns
   - Look at finos/traderx for architecture

4. **Plan scaling strategy:**
   - How many Kafka partitions?
   - How many Kafka Streams instances?
   - QuestDB disk size estimates
   - Cost projections

5. **Create ARCHITECTURE.md:**
   - System diagram
   - Data flow
   - Technology choices with rationale
   - Scaling strategy
   - Cost breakdown

---

## 🎯 Updated Project Recommendations

### Portfolio Priority Ranking

1. **EventRAG** ⭐⭐⭐⭐⭐ - Best balance, AI relevant, $0/month, 4 weeks
2. **QuantStream** ⭐⭐⭐⭐⭐ - Highest impact, enterprise scale, $60/mo, 10 weeks
3. **AgentoFlow** ⭐⭐⭐⭐ - Cutting-edge AI, high complexity, $0-5/mo, 6 weeks
4. **StreamGuard** ⭐⭐⭐⭐ - Quick win, extends existing work, $0/mo, 3 weeks

### Decision Framework

**Choose EventRAG if:**
- Want AI/ML portfolio project (2026 relevant)
- Prefer moderate complexity with excellent references
- Need $0/month deployment
- Want 3-4 week timeline
- Value ecosystem gap (few Spring Boot + Kafka + RAG projects)

**Choose QuantStream if:**
- Want to demonstrate enterprise-grade engineering
- Comfortable with very high complexity
- Interested in financial/trading domain
- Have 8-10 weeks available
- Can invest $60-100/month OR run locally with demo video
- Want to stand out with extreme scale (900K msg/sec)
- Focus on distributed systems + data engineering

**Choose AgentoFlow if:**
- Fascinated by multi-agent systems
- Want cutting-edge AI project
- Can invest 4-6 weeks
- Prefer unique over well-referenced

**Choose StreamGuard if:**
- Want to extend existing SAGA project
- Prefer shorter timeline (2-3 weeks)
- Focus on fintech/real-time ML

### Hybrid Approach

**Option: Start EventRAG, Add QuantStream Later**
1. Build EventRAG (4 weeks) - Proves AI + distributed systems
2. Add QuantStream (8 weeks) - Proves enterprise scale
3. **Result:** 2 portfolio projects, one moderate + one extreme

**Timeline:** 12 weeks total for 2 complete projects

---

