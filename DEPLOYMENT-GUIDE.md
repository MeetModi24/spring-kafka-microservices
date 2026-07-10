# Deployment Guide - Hosting AI Applications

**Research Date:** 2026-07-10  
**Context:** Deployment strategies and cost analysis for Spring Boot + AI portfolio projects

This guide covers practical deployment patterns for hosting full-stack AI applications with Spring Boot backends, React/Next.js frontends, and AI/ML services.

---

## 🎯 Key Finding

> **Zero Spring Boot AI projects** on GitHub (out of 40+ surveyed) have permanent live-hosted demo URLs in their READMEs.

**This is your opportunity:** A well-deployed Spring Boot AI portfolio project will stand out because most stop at localhost.

---

## 💰 Cost-Effective Deployment Stack (Recommended)

### The $0/Month Stack

```
┌─────────────────────────────────────────────────────────┐
│  Vercel (Frontend)                                       │
│  - Next.js / React                                       │
│  - Free tier: Unlimited hobby deploys                   │
│  - Custom subdomains, Edge CDN                          │
│  - Cost: $0/month                                       │
└─────────────────────────────────────────────────────────┘
                      ↓ HTTPS API calls
┌─────────────────────────────────────────────────────────┐
│  Fly.io (Backend Services)                              │
│  - Spring Boot microservices (Docker)                   │
│  - Free tier: 3 shared-cpu-1x VMs (256MB each)         │
│  - 3GB storage, 160GB egress                           │
│  - HTTPS + custom domain free                           │
│  - Cost: $0/month (up to 3 services)                   │
└─────────────────────────────────────────────────────────┘
                      ↓ Database connection
┌─────────────────────────────────────────────────────────┐
│  Supabase (Database + Vector Store)                     │
│  - PostgreSQL + pgvector built-in                       │
│  - Free tier: 500MB database                           │
│  - No separate vector DB service needed                 │
│  - Cost: $0/month                                       │
└─────────────────────────────────────────────────────────┘
                      ↓ API calls
┌─────────────────────────────────────────────────────────┐
│  OpenAI / Groq (LLM APIs)                               │
│  - OpenAI: Pay-as-you-go (~$0.50 for demos)            │
│  - Groq: Free, rate-limited (Llama 3.3 70B)            │
│  - Cost: ~$1/month for portfolio traffic                │
└─────────────────────────────────────────────────────────┘

**Total monthly cost: $0-1**
```

---

## 🏗️ Deployment Patterns

### Pattern A: Two-Tier Split (Recommended for Portfolio)

**Architecture:**
```
Vercel (Next.js)  ──HTTPS──▶  Fly.io (Spring Boot)  ──▶  Supabase (Postgres)
                                       │
                                       ▼
                                 OpenAI / Groq API
```

**When to use:**
- Full-stack project with separate frontend/backend
- Want modern React/Next.js frontend
- Need $0/month hosting

**Pros:**
- ✅ Both platforms have excellent free tiers
- ✅ Separate scaling for frontend/backend
- ✅ Professional architecture (mirrors production)
- ✅ Easy CI/CD (GitHub push → auto deploy)

**Cons:**
- ❌ CORS configuration required
- ❌ Two separate deployments to manage

**Setup steps:**
1. **Backend (Fly.io):**
   ```bash
   # In Spring Boot project root
   fly launch  # Auto-detects Dockerfile
   fly secrets set OPENAI_API_KEY=sk-...
   fly secrets set DATABASE_URL=postgres://...
   fly deploy
   ```

2. **Frontend (Vercel):**
   ```bash
   # In Next.js project root
   vercel  # Or connect GitHub repo in dashboard
   # Set env var: NEXT_PUBLIC_API_URL=https://your-app.fly.dev
   ```

3. **Database (Supabase):**
   - Create project in Supabase dashboard
   - Enable pgvector extension
   - Copy connection string to Fly.io secrets

### Pattern B: Single-Service HTMX

**Architecture:**
```
Fly.io (Spring Boot + HTMX SSR)  ──▶  Supabase  ──▶  OpenAI
```

**When to use:**
- Fastest path to live demo
- Prefer server-side rendering
- Avoid CORS complexity

**Pros:**
- ✅ One service, one URL, no CORS
- ✅ Minimal complexity
- ✅ Fast development

**Cons:**
- ❌ HTMX not as impressive on resume as React
- ❌ Less "portfolio-photogenic"

**Reference:** `habuma/spring-ai-examples/spring-ai-htmx-mcp`

### Pattern C: Railway Monorepo

**Architecture:**
```
Railway (detects Spring Boot + React from monorepo)  ──▶  Railway Postgres
```

**When to use:**
- Want smoothest GitHub → deploy pipeline
- Willing to pay $5/month
- Prefer monorepo structure

**Pros:**
- ✅ Push to main → auto rebuild
- ✅ Native buildpacks (no Dockerfile needed)
- ✅ Excellent DX

**Cons:**
- ❌ $5/month (free tier credit runs out in ~7 days)
- ❌ More expensive than Fly.io + Vercel combo

**Cost:** $5/month Hobby plan

### Pattern D: Render + Neon

**Architecture:**
```
Render (Spring Boot)  ──▶  Neon (Postgres + pgvector)
```

**When to use:**
- Alternative to Fly.io
- Simple deployment needs

**Pros:**
- ✅ Simple setup
- ✅ Neon has good Postgres free tier (10GB)

**Cons:**
- ❌ **Render free tier sleeps after 15 min** (30s cold start)
- ❌ Need $7/month to keep warm
- ❌ Sleeping demos kill portfolio impact

**Cost:** $0/month (with sleep) or $7/month (always-on)

**Verdict:** Only use if you upgrade to paid ($7/mo). Sleeping demos fail in interviews.

### Pattern E: AWS for Resume Signal

**Architecture:**
```
CloudFront/S3 (Frontend)  ──▶  ECS Fargate (Backend)  ──▶  RDS Postgres
```

**When to use:**
- Specifically want AWS on resume
- Interviewing at AWS-heavy companies
- Need to discuss VPC/IAM/task definitions

**Pros:**
- ✅ AWS resume signal
- ✅ Production-grade architecture
- ✅ Mirrors real infrastructure

**Cons:**
- ❌ $20-40/month at idle
- ❌ Much more complex
- ❌ Overkill for portfolio

**Cost:** ~$30/month

**Verdict:** Only if AWS experience is specifically required for target roles.

---

## 💵 Platform Comparison

| Platform | Free Tier | Best For | Real Monthly Cost |
|----------|-----------|----------|-------------------|
| **Fly.io** | 3 tiny VMs, 3GB storage, 160GB egress | Spring Boot backend | $0-2 |
| **Railway** | $5 trial credit → paid | Monorepo w/ CI/CD | $5 (Hobby plan) |
| **Render** | Free tier sleeps | Simple, with $7 upgrade | $0 (sleeps) / $7 (warm) |
| **Vercel** | Unlimited hobby | Next.js/React | $0 |
| **Netlify** | Similar to Vercel | Static/JAM | $0 |
| **Supabase** | 500MB Postgres+pgvector | Full DB stack | $0 |
| **Neon** | 10GB serverless Postgres | Postgres-only | $0 |
| **Upstash** | 10k Kafka msgs/day | Managed Kafka | $0 |
| **AWS Fargate** | 12-month limited | Resume signal | $20-40 |
| **Heroku** | No free tier | Legacy | $7+ (skip it) |

### Recommended Combinations

**Best for Portfolio (EventRAG):**
- Fly.io (3 Spring Boot services) + Vercel (Next.js) + Supabase (DB) + Upstash (Kafka)
- **Cost:** $0/month + LLM API (~$1)

**Simplest (Single service):**
- Fly.io (Spring Boot + HTMX) + Supabase (DB)
- **Cost:** $0/month + LLM API (~$1)

**Best DX (Monorepo):**
- Railway (monorepo) + Railway Postgres
- **Cost:** $5/month + LLM API (~$1)

---

## 🔧 Fly.io Deployment Deep Dive

### Why Fly.io for Spring Boot?

1. **True free tier** (not trial credits)
2. **Docker-native** (no buildpack hacks)
3. **Fast global edge** (low latency)
4. **Simple CLI** (`fly launch`, `fly deploy`)
5. **Persistent volumes** (if needed)
6. **Custom domains free** (HTTPS included)

### Spring Boot Dockerfile for Fly.io

**Multi-stage Dockerfile (optimized):**

```dockerfile
# Build stage
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app
COPY .mvn/ .mvn
COPY mvnw pom.xml ./
RUN ./mvnw dependency:go-offline
COPY src ./src
RUN ./mvnw package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar

# Fly.io health check endpoint
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0"

ENTRYPOINT ["java", "-jar", "app.jar"]
```

**Key optimizations:**
- Multi-stage build (smaller final image ~150MB)
- JRE instead of JDK (60% smaller)
- `MaxRAMPercentage=75` prevents OOM on 256MB VMs
- Dependency caching for faster rebuilds

### fly.toml Configuration

```toml
app = "your-app-name"
primary_region = "sjc"  # San Jose (closest to you)

[build]
  dockerfile = "Dockerfile"

[env]
  SERVER_PORT = "8080"
  SPRING_PROFILES_ACTIVE = "production"

[http_service]
  internal_port = 8080
  force_https = true
  auto_stop_machines = true
  auto_start_machines = true
  min_machines_running = 0  # Scale to zero when idle

[[http_service.checks]]
  grace_period = "10s"
  interval = "30s"
  method = "GET"
  timeout = "5s"
  path = "/actuator/health"

[[vm]]
  cpu_kind = "shared"
  cpus = 1
  memory_mb = 256
```

**Key settings:**
- `auto_stop_machines = true` - Scale to zero (save resources)
- Health check on `/actuator/health` (requires Spring Actuator)
- `force_https = true` - Free SSL

### Fly.io Secrets Management

```bash
# Set secrets (not in code!)
fly secrets set OPENAI_API_KEY=sk-...
fly secrets set DATABASE_URL=postgresql://...
fly secrets set SPRING_DATASOURCE_PASSWORD=...

# List secrets
fly secrets list

# Remove secret
fly secrets unset OPENAI_API_KEY
```

### Common Fly.io Issues & Fixes

#### Issue 1: OOM Killed
**Symptom:** Container crashes, logs show "Out of memory"

**Fix:** Add to Dockerfile:
```dockerfile
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"
```

#### Issue 2: Cold Start Too Slow
**Symptom:** 4-6 second startup on first request

**Option A - Keep one machine running:**
```bash
fly scale count 1 --min-machines-running=1
```
(Uses more free tier resources)

**Option B - GraalVM native image:**
```dockerfile
# Use GraalVM for 100ms startup
FROM ghcr.io/graalvm/native-image:21 AS builder
# ... build native image
```
(Requires Spring Boot 3 native support)

#### Issue 3: Slow Initial Response
**Symptom:** First request after cold start takes long

**Fix:** Enable Spring Boot lazy initialization in production:
```yaml
spring:
  main:
    lazy-initialization: true
```

---

## 🌐 Vercel Deployment Deep Dive

### Why Vercel for Next.js?

1. **Zero-config** Next.js deployment
2. **Unlimited hobby projects** (truly free)
3. **Edge network** (fast globally)
4. **Auto HTTPS** on custom domains
5. **GitHub integration** (push to deploy)
6. **Environment variables** (secure secrets)

### Next.js Project Setup

**Required environment variable:**
```bash
# .env.local (for local dev)
NEXT_PUBLIC_API_URL=http://localhost:8080

# .env.production (Vercel dashboard)
NEXT_PUBLIC_API_URL=https://your-backend.fly.dev
```

### Deployment Steps

**Option A - CLI:**
```bash
npm install -g vercel
vercel  # Follow prompts
vercel --prod  # Deploy to production
```

**Option B - GitHub (recommended):**
1. Push code to GitHub
2. Go to vercel.com → Import Project
3. Connect GitHub repo
4. Set env vars in Vercel dashboard
5. Push to main → auto deploy

### CORS Configuration (Critical!)

**In Spring Boot backend:**

```java
@Configuration
public class CorsConfig {
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**")
                        .allowedOrigins(
                            "http://localhost:3000",  // Local dev
                            "https://your-app.vercel.app"  // Production
                        )
                        .allowedMethods("GET", "POST", "PUT", "DELETE")
                        .allowedHeaders("*")
                        .allowCredentials(true);
            }
        };
    }
}
```

**Or use annotation:**
```java
@RestController
@CrossOrigin(origins = {
    "http://localhost:3000",
    "https://your-app.vercel.app"
})
@RequestMapping("/api")
public class MyController {
    // ...
}
```

**Common CORS errors:**
- "No 'Access-Control-Allow-Origin' header" → Add CORS config
- "Preflight request failed" → Allow OPTIONS method
- "Credentials mode" → Set `allowCredentials(true)`

---

## 🗄️ Database Deployment

### Supabase (PostgreSQL + pgvector)

**Why Supabase:**
- ✅ PostgreSQL + pgvector built-in
- ✅ No separate vector DB service
- ✅ 500MB free tier
- ✅ Web dashboard for SQL
- ✅ Auth/Storage included (if needed)

**Setup:**
1. Go to supabase.com → New Project
2. Wait for provisioning (~2 min)
3. Enable pgvector:
   ```sql
   CREATE EXTENSION IF NOT EXISTS vector;
   ```
4. Copy connection string
5. Add to Fly.io secrets:
   ```bash
   fly secrets set DATABASE_URL=postgresql://...
   ```

**Connection pooling:**
Supabase provides two URLs:
- Direct: `postgresql://...` (use for migrations)
- Pooler: `postgresql://...6543/postgres` (use for app)

Use pooler URL in production (handles connections better).

### Neon (Alternative)

**Why Neon:**
- ✅ Serverless Postgres (autoscale)
- ✅ 10GB free tier (vs Supabase 500MB)
- ✅ pgvector supported
- ✅ Branching (like Git for databases)

**When to use:** Larger database needs (>500MB)

**Setup:** Similar to Supabase, create project → enable pgvector → copy URL

---

## 📨 Kafka Deployment

### Upstash Kafka (Recommended)

**Why Upstash:**
- ✅ Managed Kafka (no ops)
- ✅ Free tier: 10,000 msgs/day
- ✅ REST API + native protocol
- ✅ Regional deployment

**Free tier limits:**
- 10,000 messages/day
- 10 MB max message size
- 1 GB max storage
- 7 day retention

**Setup:**
1. Go to upstash.com → Create Kafka cluster
2. Create topics via dashboard
3. Get credentials
4. Configure Spring Boot:
   ```yaml
   spring:
     kafka:
       bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
       properties:
         sasl.mechanism: SCRAM-SHA-256
         security.protocol: SASL_SSL
         sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
   ```

**Good for:** EventRAG project (document processing pipeline)

### Redpanda Cloud (Alternative)

**Free tier:** 10 GB storage, 10 Mbps throughput

**When to use:** Need more throughput than Upstash

### Self-hosted on Fly.io (Not Recommended)

**Why avoid:** Kafka needs persistent volumes, multiple brokers, ZooKeeper. Complex and expensive on Fly.io. Use managed service instead.

---

## 💸 LLM API Cost Analysis

### OpenAI Pricing (as of 2026)

**GPT-4o-mini (recommended for portfolio):**
- Input: $0.15 / 1M tokens
- Output: $0.60 / 1M tokens

**text-embedding-3-small (embeddings):**
- $0.02 / 1M tokens

**Example costs for EventRAG:**
- Index 1,000 documents (500 pages): ~$0.05 one-time
- 100 chat queries (10 demos): ~$0.10
- **Total for portfolio demos: ~$1/month**

### Groq (Free Alternative)

**Llama 3.3 70B:**
- Free with rate limits
- Blazingly fast (~500 tokens/sec)
- Good for demos/testing

**Rate limits:**
- 30 requests/min
- 14,400 tokens/min

**When to use:** Testing, demos, fallback when OpenAI budget runs out

### Anthropic Claude

**Claude Haiku:**
- Input: $0.25 / 1M tokens
- Output: $1.25 / 1M tokens

**When to use:** Similar cost to GPT-4o-mini, alternative provider

### Cost Management Tips

1. **Use mini models** (GPT-4o-mini, not GPT-4)
2. **Cache embeddings** (don't re-embed same docs)
3. **Limit context window** (fewer tokens = less cost)
4. **Use Groq for testing** (free, fast)
5. **Set spending limits** in OpenAI dashboard

---

## 🔐 Security Best Practices

### Never Commit Secrets

**Bad:**
```yaml
# application.yml (DO NOT DO THIS)
openai:
  api-key: sk-proj-abc123...
```

**Good:**
```yaml
# application.yml
openai:
  api-key: ${OPENAI_API_KEY}
```

Then set via environment:
```bash
fly secrets set OPENAI_API_KEY=sk-...
```

### Use .gitignore

```gitignore
.env.local
.env.production
application-local.yml
**/secrets/
```

### Rotate Keys

- Don't use same OpenAI key for dev and prod
- Create separate keys in OpenAI dashboard
- Rotate keys if accidentally committed

---

## 📊 Deployment Checklist

### Before Deploying

- [ ] Add health check endpoint (`/actuator/health`)
- [ ] Configure CORS for frontend domain
- [ ] Externalize all secrets to environment variables
- [ ] Add .gitignore for secrets
- [ ] Test locally with Docker Compose
- [ ] Write Dockerfile (multi-stage build)
- [ ] Add logging configuration
- [ ] Configure connection pooling for database
- [ ] Set up error handling for API failures

### Fly.io Deployment

- [ ] Install Fly.io CLI (`brew install flyctl`)
- [ ] Authenticate (`fly auth login`)
- [ ] Create Dockerfile in project root
- [ ] Run `fly launch` (creates fly.toml)
- [ ] Set secrets (`fly secrets set KEY=value`)
- [ ] Deploy (`fly deploy`)
- [ ] Check logs (`fly logs`)
- [ ] Test health endpoint (`curl https://your-app.fly.dev/actuator/health`)
- [ ] Test API endpoints
- [ ] Monitor resources (`fly status`)

### Vercel Deployment

- [ ] Create Next.js project
- [ ] Add API URL to env vars
- [ ] Push to GitHub
- [ ] Connect repo in Vercel dashboard
- [ ] Set production env vars in Vercel
- [ ] Deploy
- [ ] Test frontend → backend connection
- [ ] Verify CORS working
- [ ] Check console for errors

### Database Setup

- [ ] Create Supabase/Neon project
- [ ] Enable pgvector extension
- [ ] Run migrations (create tables)
- [ ] Add indexes for performance
- [ ] Copy connection string to Fly.io secrets
- [ ] Test connection from backend
- [ ] Set up connection pooling

### Post-Deployment

- [ ] Add custom domain (optional)
- [ ] Set up monitoring/alerts
- [ ] Test all features end-to-end
- [ ] Check API costs (OpenAI dashboard)
- [ ] Document deployment process
- [ ] Share live URL in README
- [ ] Record demo video
- [ ] Update portfolio with link

---

## 🎬 Demo Video Tips

### Recording the Demo

1. **Use Loom or QuickTime** (free screen recording)
2. **Keep it under 3 minutes** (attention span)
3. **Show the live URL** (prove it's deployed)
4. **Demonstrate key features** (upload → process → query)
5. **Show real-time aspects** (WebSocket updates, streaming)
6. **Open network tab** (show API calls, no errors)
7. **Explain architecture briefly** (Kafka pipeline, microservices)

### GIF for README

Use for quick preview:
```bash
# Record GIF with Kap (Mac)
brew install kap

# Or use Gifski to convert video
brew install gifski
gifski input.mp4 -o demo.gif
```

Add to README:
```markdown
## Live Demo

**URL:** https://your-app.vercel.app

![Demo](./docs/demo.gif)
```

---

## 📚 Additional Resources

### Fly.io Docs
- Getting started: https://fly.io/docs/getting-started/
- Spring Boot guide: https://fly.io/docs/languages-and-frameworks/spring-boot/
- Scaling: https://fly.io/docs/reference/scaling/

### Vercel Docs
- Next.js deployment: https://vercel.com/docs/frameworks/nextjs
- Environment variables: https://vercel.com/docs/concepts/projects/environment-variables

### Supabase Docs
- Quick start: https://supabase.com/docs/guides/getting-started
- pgvector guide: https://supabase.com/docs/guides/ai/vector-columns

---

## 🎯 Quick Start: Deploy EventRAG

**Complete deployment in 30 minutes:**

```bash
# 1. Backend (Fly.io)
cd backend
fly launch --name eventrag-api
fly secrets set OPENAI_API_KEY=sk-...
fly secrets set DATABASE_URL=postgresql://...
fly deploy

# 2. Frontend (Vercel)
cd frontend
vercel
# Set env: NEXT_PUBLIC_API_URL=https://eventrag-api.fly.dev

# 3. Test
curl https://eventrag-api.fly.dev/actuator/health
open https://your-app.vercel.app
```

**Total cost: $0/month** (plus ~$1 in API usage)

---

**Last Updated:** 2026-07-10  
**Next:** Deploy your chosen project using these patterns
