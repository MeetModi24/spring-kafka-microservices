# Reference Projects - GitHub Research Findings

**Research Date:** 2026-07-10  
**Sources:** 3 specialized agents researching RAG systems, ML+Kafka platforms, and deployed demos

This document catalogs all reference projects found during research for next portfolio project.

---

## 📁 Category Index

1. [RAG & LLM Systems](#rag--llm-systems)
2. [Kafka + ML Streaming](#kafka--ml-streaming)
3. [Multi-Agent Systems](#multi-agent-systems)
4. [Deployment Examples](#deployment-examples)
5. [Foundational Frameworks](#foundational-frameworks)

---

## 🤖 RAG & LLM Systems

### 1. langchain4j-aideepin ⭐ TOP REFERENCE
- **URL:** https://github.com/moyangzhan/langchain4j-aideepin
- **Stars:** 1,300
- **Last Updated:** 2026-07
- **Stack:** Spring Boot + LangChain4j backend, **Vue3 frontend** (separate repo), PostgreSQL + pgvector, Redis
- **Features:** Chat, RAG knowledge base, workflow orchestration, MCP marketplace, TTS/ASR, long-term memory, drawing
- **Deployment:** Docker Compose provided
- **Why Study:** Closest to production-grade full-stack RAG app in Java. Clean modular architecture.
- **Note:** Documentation partially Chinese but code is readable

### 2. piomin/spring-ai-showcase
- **URL:** https://github.com/piomin/spring-ai-showcase
- **Stars:** 300
- **Last Updated:** Active
- **Stack:** Spring Boot, Spring AI, OpenAI/Mistral/Ollama/Azure, Pinecone
- **Features:** Prompts, chat memory, structured output, function calling, RAG, image generation
- **Why Study:** Best modular Spring AI examples. Author blogs each example with tutorials.
- **Deployment:** Well-documented, deployable

### 3. spring-petclinic/spring-petclinic-ai
- **URL:** https://github.com/spring-petclinic/spring-petclinic-ai
- **Stars:** 400
- **Last Updated:** Active
- **Stack:** Spring Boot + Spring AI, OpenAI/Azure OpenAI, H2/Postgres
- **Features:** Chatbot on classic Spring app, function calling, JPA integration
- **Why Study:** Canonical Spring AI example, official Spring.io blog series, clean code
- **Deployment:** Docker-ready, easy Railway/Render deployment

### 4. ThomasVitale/llm-apps-java-spring-ai
- **URL:** https://github.com/ThomasVitale/llm-apps-java-spring-ai
- **Stars:** 757
- **Last Updated:** Active
- **Stack:** Spring Boot 3 + Spring AI, various LLM providers
- **Features:** RAG, function calling, observability, clean examples
- **Why Study:** Best "learn by reading" repo. Thomas Vitale is Spring AI expert.
- **Deployment:** Each example is self-contained

### 5. HemantMedhsia/RAG
- **URL:** https://github.com/HemantMedhsia/RAG
- **Stars:** Fresh project
- **Stack:** Spring Boot + React + OpenAI + pgvector
- **Features:** PDF RAG, full-stack with React frontend
- **Why Study:** Full-stack shape you want (Spring Boot + React + pgvector)
- **Deployment:** Supabase pgvector + Railway backend + Vercel frontend

### 6. interview-guide (Snailclimb)
- **URL:** https://github.com/Snailclimb/interview-guide
- **Stars:** 2,600
- **Last Updated:** 2026-07
- **Stack:** Spring Boot 4.1, Java 21, Spring AI 2.0, PostgreSQL + pgvector
- **Features:** Resume analysis, AI mock interviews, RAG knowledge base
- **Why Study:** Modern stack (Boot 4.1, Java 21, Spring AI 2.0), production-shaped

### 7. yu-ai-code-mother (liyupi)
- **URL:** https://github.com/liyupi/yu-ai-code-mother
- **Stars:** 1,800
- **Stack:** Spring Boot 3 + LangChain4j, **Vue 3** frontend, Spring Cloud microservices
- **Features:** AI code generation, LangGraph4j workflows, multi-agent routing, SSE streaming
- **Why Study:** **Microservices architecture** (matches your existing project), observability (Prometheus+Grafana)
- **Note:** Chinese docs but architecture diagrams are universal

### 8. AIFlowy
- **URL:** https://github.com/aiflowy/aiflowy
- **Stars:** 889
- **Last Updated:** 2026-07
- **Stack:** Java + Spring Boot, enterprise AI platform
- **Features:** Visual workflow builder, RAG, agent platform, multi-tenant
- **Why Study:** Enterprise-grade architecture, workflow engine patterns

### 9. mateclaw
- **URL:** https://github.com/mateaix/mateclaw
- **Stars:** 748
- **Stack:** Spring AI Alibaba, multi-agent orchestration, MCP protocol
- **Features:** Multi-agent + MCP-first design, skills + memory
- **Why Study:** Multi-agent patterns, "second brain" style app

### 10. EDDI (labsai)
- **URL:** https://github.com/labsai/EDDI
- **Stars:** 363
- **Stack:** Quarkus (not Spring, but JVM) + LangChain4j
- **Features:** Config-driven agents (JSON → agent), 12+ LLM providers, MCP/A2A protocols, GDPR/HIPAA compliance
- **Why Study:** Enterprise compliance angle, well-architected

---

## 🌊 Kafka + ML Streaming

### 11. kaiwaehner/kafka-streams-machine-learning-examples ⭐ FOUNDATIONAL
- **URL:** https://github.com/kaiwaehner/kafka-streams-machine-learning-examples
- **Stars:** 911
- **Last Updated:** Active
- **Stack:** Java, Kafka Streams, H2O.ai, TensorFlow (Java), Keras, DeepLearning4J
- **Approach:** Models trained in Python, exported, loaded into JVM Kafka Streams topology (embedded inference)
- **Why Study:** Canonical Java/Kafka/ML reference, shows model-as-code pattern
- **Deployment:** Embedded vs remote inference patterns

### 12. kaiwaehner/tensorflow-serving-java-grpc-kafka-streams
- **URL:** https://github.com/kaiwaehner/tensorflow-serving-java-grpc-kafka-streams
- **Stars:** 149
- **Stack:** Java, Kafka Streams, gRPC, TensorFlow Serving
- **Approach:** Kafka Streams calls TF Serving via gRPC (RPC pattern)
- **Why Study:** Real MLOps pattern (mirrors Ray Serve, Seldon, KServe)

### 13. kaiwaehner/ksql-udf-deep-learning-mqtt-iot
- **URL:** https://github.com/kaiwaehner/ksql-udf-deep-learning-mqtt-iot
- **Stars:** 306
- **Stack:** KSQL, Deep Learning UDF, MQTT, Kafka
- **Features:** Anomaly detection on IoT streams via KSQL UDF with DL model
- **Why Study:** ML-as-SQL-function pattern, visual demo potential

### 14. kaiwaehner/hivemq-mqtt-tensorflow-kafka-realtime-iot-machine-learning
- **URL:** https://github.com/kaiwaehner/hivemq-mqtt-tensorflow-kafka-realtime-iot-machine-learning
- **Stars:** 419
- **Stack:** MQTT, Kafka, TensorFlow, HiveMQ
- **Features:** Real-time IoT ML, integrated end-to-end demo
- **Why Study:** Complete IoT → Kafka → ML pipeline

### 15. Ramanjaneyareddy/payments-flow ⭐ CLOSEST TO YOUR STACK
- **URL:** https://github.com/Ramanjaneyareddy/payments-flow
- **Stars:** Active project
- **Stack:** Spring Boot 3 + Java 17, Kafka, Redis, Spring AI
- **Features:** Event-driven payments platform, real-time fraud detection, Prometheus/Grafana, Docker-ready microservices
- **Why Study:** **Closest match to your codebase** (Spring Boot + Kafka + microservices + AI)

### 16. plsyz/finstream-ai
- **URL:** https://github.com/plsyz/finstream-ai
- **Stars:** Active project
- **Stack:** Spring Boot + Spring AI + Kafka + Resilience4j, pgvector, Neo4j
- **Features:** Multi-agent transaction pipeline, PII sanitization, GraphRAG fraud detection
- **Why Study:** RAG + graph DB + Kafka is impressive scope

### 17. mouse1999/nexus-agentic-commerce
- **URL:** https://github.com/mouse1999/nexus-agentic-commerce
- **Stars:** Active project
- **Stack:** Spring AI + Kafka + observability
- **Features:** AI shopping agent orchestrates product discovery, inventory, orders
- **Why Study:** Agent orchestration + saga aligns with your existing OrderOrchestrationService

### 18. juanmahiav/jaip-spring-ai-event-driven-rag
- **URL:** https://github.com/juanmahiav/jaip-spring-ai-event-driven-rag
- **Stars:** Active project
- **Stack:** Spring AI + Kafka + Ollama (local LLM) + Qdrant via MCP
- **Features:** Event-driven RAG with local LLM
- **Why Study:** Local-LLM RAG over events is differentiated

### 19. gauravpv/Intellidesk
- **URL:** https://github.com/gauravpv/Intellidesk
- **Stack:** Spring Boot microservices + Spring AI + Kafka + Redis + Postgres
- **Features:** AI customer-support triage
- **Why Study:** Classic use case, easy to demo

### 20. altayyeles/Real-Time-Fraud-Detection-Kafka-AI
- **URL:** https://github.com/altayyeles/Real-Time-Fraud-Detection-Kafka-AI
- **Stack:** Python (FastAPI), Kafka, IsolationForest, WebSocket dashboard, SQLite
- **Features:** Transaction simulator + fraud detection + live dashboard
- **Why Study:** **Steal the WebSocket live-dashboard UI pattern**, reimplement backend in Spring
- **Note:** Python, but UI/architecture patterns are valuable

### 21. forUAi/fraudshield-ml
- **URL:** https://github.com/forUAi/fraudshield-ml
- **Stack:** Python, Kafka, Feast, XGBoost, PyTorch, Ray Serve, MLflow, Evidently AI
- **Features:** Production-grade ML platform with feature store, model registry, drift monitoring
- **Why Study:** Full MLOps stack reference
- **Note:** Python, but architecture is valuable

---

## 🤝 Multi-Agent Systems

### 22. langgraph4j/langgraph4j
- **URL:** https://github.com/langgraph4j/langgraph4j
- **Stars:** 1,800
- **Last Updated:** Active
- **Stack:** Pure Java, works with Spring Boot
- **Features:** Stateful multi-agent workflows, Java port of LangGraph
- **Why Study:** Build agentic orchestration in Java

### 23. cool-icu0/xgent
- **URL:** https://github.com/cool-icu0/xgent
- **Stars:** 113
- **Stack:** Spring AI + LangChain4j + Google ADK, MCP
- **Features:** Multi-agent orchestration with MCP protocol
- **Why Study:** Enterprise complexity, shows multi-agent architecture

---

## 🚀 Deployment Examples

### 24. habuma/spring-ai-examples (Craig Walls)
- **URL:** https://github.com/habuma/spring-ai-examples
- **Stars:** 400
- **Author:** Craig Walls (Spring AI in Action book)
- **Stack:** Spring Boot + Spring AI, includes HTMX examples
- **Features:** Dozens of examples, HTMX MCP chat UI (single-service deploy)
- **Why Study:** **HTMX example = single JAR deployment** (simplest hosting)
- **Deployment:** Single service, easy Fly.io/Railway

### 25. tzolov/playground-flight-booking
- **URL:** https://github.com/tzolov/playground-flight-booking
- **Stars:** 600
- **Stack:** Vaadin UI + Spring AI
- **Features:** Domain-specific AI agent (flight booking assistant), function-calling patterns
- **Why Study:** Single JAR, good Fly.io/Railway candidate
- **Deployment:** Self-contained

### 26. aws-samples/sample-once-upon-spring-ai
- **URL:** https://github.com/aws-samples/sample-once-upon-spring-ai
- **Stars:** 200
- **Stack:** Spring AI + AWS Bedrock, Java 25
- **Features:** AWS-official Spring AI agentic tutorial
- **Why Study:** Serverless patterns, AWS integration
- **Note:** Bedrock is paid, but patterns are valuable

---

## 🏗️ Foundational Frameworks

### 27. Spring AI (official) ⭐ MUST STUDY
- **URL:** https://github.com/spring-projects/spring-ai
- **Stars:** 9,100
- **Last Updated:** Daily
- **Stack:** Spring Boot 3.x, all major LLM providers, all major vector stores
- **Why Study:** **THE canonical Java LLM framework**. Learn its abstractions.
- **Companion:** spring-ai-examples (1.4k stars) - official reference implementations

### 28. LangChain4j ⭐ ALTERNATIVE FRAMEWORK
- **URL:** https://github.com/langchain4j/langchain4j
- **Stars:** 12,500
- **Last Updated:** Active
- **Stack:** Pure Java, works with Spring Boot + Quarkus, all major providers
- **Why Study:** Broader ecosystem than Spring AI, more integrations
- **Companion:** langchain4j-examples (1.8k stars)

### 29. spring-ai-alibaba
- **URL:** https://github.com/alibaba/spring-ai-alibaba
- **Stars:** 10,300
- **Stack:** Alibaba's Spring AI extensions
- **Why Study:** Production usage at scale, enterprise patterns

### 30. DataAgent (Alibaba)
- **URL:** https://github.com/spring-ai-alibaba/DataAgent
- **Stars:** 2,200
- **Stack:** Spring AI Alibaba + agent framework
- **Features:** Text2SQL, data analytics agent, has UI
- **Why Study:** Data-focused agent patterns, deployable

---

## 📚 Curated Lists & Resources

### 31. awesome-spring-ai (community)
- **URL:** https://github.com/spring-ai-community/awesome-spring-ai
- **Stars:** 808
- **Content:** Curated index of Spring AI projects
- **Why Study:** Ongoing catalog for discovery

### 32. awesome-spring-ai (Dan Vega)
- **URL:** https://github.com/danvega/awesome-spring-ai
- **Stars:** 279
- **Content:** Slimmer, actively-updated list by Spring expert Dan Vega
- **Why Study:** High-quality curation

### 33. Kotlin/Kotlin-AI-Examples
- **URL:** https://github.com/Kotlin/Kotlin-AI-Examples
- **Stars:** 265
- **Stack:** Kotlin, Spring AI + LangChain4j, notebooks
- **Why Study:** JetBrains-maintained, reference quality
- **Note:** Kotlin but patterns apply to Java

---

## 🏆 Classic Distributed Systems References

These aren't AI-focused but provide excellent distributed systems patterns:

### 34. berndruecker/flowing-retail
- **URL:** https://github.com/berndruecker/flowing-retail
- **Stars:** 1,300
- **Stack:** Spring Boot, Kafka, Camunda, Zeebe, RabbitMQ
- **Features:** **Choreography vs orchestration side-by-side**, multiple architectural solutions
- **Why Study:** Same problem, multiple approaches with tradeoffs. Widely cited.

### 35. eventuate-tram/eventuate-tram-sagas
- **URL:** https://github.com/eventuate-tram/eventuate-tram-sagas
- **Stars:** 1,100
- **Stack:** Spring Boot, Kafka, MySQL, Debezium-style CDC
- **Features:** **Transactional outbox pattern**, orchestration-based sagas
- **Why Study:** Canonical reference for outbox + reliable event publishing

### 36. piomin/sample-spring-kafka-microservices
- **URL:** https://github.com/piomin/sample-spring-kafka-microservices
- **Stars:** 419
- **Stack:** Spring Boot, Kafka Streams
- **Features:** SAGA with **Kafka Streams** (not plain consumers) for orchestration
- **Why Study:** Step above typical consumer sagas

### 37. idugalic/digital-restaurant
- **URL:** https://github.com/idugalic/digital-restaurant
- **Stars:** 319
- **Stack:** Kotlin, Spring, Axon, Kafka, RabbitMQ
- **Features:** Full DDD with **CQRS + Event Sourcing**, hexagonal architecture
- **Why Study:** Rare complete DDD/ES/CQRS example

---

## 📊 Reference Project Usage Matrix

| Project | For EventRAG | For StreamGuard | For AgentoFlow |
|---------|--------------|-----------------|----------------|
| langchain4j-aideepin | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| piomin/spring-ai-showcase | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| spring-petclinic-ai | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| kaiwaehner/kafka-ml | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| payments-flow | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| langgraph4j | ⭐⭐ | ⭐ | ⭐⭐⭐⭐⭐ |
| nexus-agentic-commerce | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| eventuate-tram-sagas | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| Your spring-kafka-microservices | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |

---

## 🎯 How to Use These References

### For EventRAG Project:
1. **Study first:** langchain4j-aideepin, piomin/spring-ai-showcase, spring-petclinic-ai
2. **Clone locally:** Set up reference repos side-by-side with your project
3. **Read code:** Focus on RAG pipeline, pgvector integration, frontend patterns
4. **Adapt patterns:** Don't copy, understand and adapt to your architecture

### For StreamGuard Project:
1. **Study first:** kaiwaehner/kafka-ml-examples, payments-flow, your own saga project
2. **Focus on:** Kafka Streams aggregations, ML model integration, dashboard UI
3. **Adapt from:** Real-Time-Fraud-Detection UI patterns

### For AgentoFlow Project:
1. **Study first:** langgraph4j, nexus-agentic-commerce, cool-icu0/xgent
2. **Focus on:** Agent orchestration, inter-agent communication, workflow patterns
3. **Adapt from:** Your existing saga orchestration patterns

### General Research Strategy:
1. **Clone reference repos** to ~/references/ folder
2. **Read READMEs** and architecture docs first
3. **Run examples locally** to understand behavior
4. **Study specific patterns** you need (don't try to understand everything)
5. **Adapt, don't copy** - understand principles and apply to your domain

---

## 📝 Notes

- **Language note:** Some high-quality projects (yu-ai-code-mother, interview-guide) have Chinese documentation but readable code and architecture diagrams
- **Framework choice:** Spring AI is official and well-integrated, LangChain4j has broader ecosystem. Either works.
- **Kai Waehner's projects:** Foundational for Kafka + ML patterns, multiple repos by same author
- **Python projects included:** Some Python projects (fraud detection dashboard) are valuable for UI/architecture patterns, not direct code reuse
- **Stars are snapshot:** GitHub stars as of research date, may have changed

---

**Last Updated:** 2026-07-10  
**Next:** Choose project and clone relevant references to study
