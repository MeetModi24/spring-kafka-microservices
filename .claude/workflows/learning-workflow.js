export const meta = {
  name: 'spring-kafka-learning-workflow',
  description: 'Explore reference repository and create comprehensive Spring Boot + Kafka learning plan',
  phases: [
    { title: 'Explore', detail: 'Analyze reference repository structure, services, and patterns' },
    { title: 'Design', detail: 'Create detailed learning curriculum with LLD concepts' },
  ],
}

phase('Explore')

const explorations = await parallel([
  () => agent(
    'Explore the GitHub repository piomin/sample-spring-kafka-microservices. Research: 1) Overall architecture - how many microservices, what does each do 2) Technology stack - Spring Boot version, Kafka version, dependencies 3) Communication patterns - how services interact via Kafka 4) Project structure - folder layout, common patterns 5) Key design patterns used. Use web search and WebFetch. Return comprehensive analysis with all services, tech stack details, and patterns identified.',
    { label: 'Explore repo architecture' }
  ),

  () => agent(
    'Research essential Spring Boot concepts for Kafka microservices at beginner-intermediate level: 1) Spring Boot fundamentals (annotations, auto-configuration) 2) Dependency Injection patterns 3) REST controllers 4) Spring Kafka (producer/consumer) 5) Configuration management 6) Error handling. Return detailed explanations with code examples for each concept.',
    { label: 'Research Spring Boot' }
  ),

  () => agent(
    'Research Low-Level Design patterns for Spring Boot microservices: 1) Layered Architecture (Controller/Service/Repository) 2) DTO pattern 3) Event-Driven patterns 4) Saga pattern 5) Idempotency patterns 6) Circuit Breaker 7) Factory/Builder patterns. For each: describe what it is, why use it, when to apply, how to implement.',
    { label: 'Research LLD patterns' }
  ),

  () => agent(
    'Research Kafka concepts for microservices: 1) Topics/Partitions/Consumer Groups 2) Producer patterns (sync/async) 3) Consumer patterns (commit strategies) 4) Message serialization 5) Idempotency and exactly-once 6) Event choreography vs orchestration 7) Dead Letter Topics. Return beginner-friendly comprehensive explanations with use cases and best practices.',
    { label: 'Research Kafka concepts' }
  )
])

phase('Design')

const curriculumInput = JSON.stringify({
  repo: explorations[0],
  springBoot: explorations[1],
  lld: explorations[2],
  kafka: explorations[3]
}, null, 2)

const curriculum = await agent(
  `Create a detailed 12-week learning curriculum for Spring Boot + Kafka microservices based on these research findings: ${curriculumInput}. Each week must have: learning objectives, core concepts (detailed), design patterns to learn, hands-on task, review questions. Follow the reference repository structure progressively. Make it comprehensive for deep learning.`,
  { label: 'Create curriculum' }
)

return {
  repositoryAnalysis: explorations[0],
  springBootConcepts: explorations[1],
  lldPatterns: explorations[2],
  kafkaConcepts: explorations[3],
  curriculum: curriculum
}
