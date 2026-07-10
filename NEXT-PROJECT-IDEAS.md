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
