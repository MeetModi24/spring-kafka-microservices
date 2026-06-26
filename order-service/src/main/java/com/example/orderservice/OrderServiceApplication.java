package com.example.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Order Service application.
 *
 * DEEP DIVE: @SpringBootApplication Annotation
 * ===========================================
 *
 * This is a COMPOSITE annotation that combines three annotations:
 *
 * 1. @SpringBootConfiguration
 *    - Marks this class as a configuration class
 *    - Allows defining @Bean methods in this class
 *    - Equivalent to @Configuration from Spring Framework
 *
 * 2. @EnableAutoConfiguration
 *    - THE MAGIC OF SPRING BOOT!
 *    - Scans classpath for dependencies and auto-configures beans
 *    - Example: Sees spring-web → configures DispatcherServlet, embedded Tomcat
 *    - Example: Sees spring-kafka → configures KafkaTemplate, consumer factories
 *    - Uses conditional logic (@ConditionalOnClass, @ConditionalOnMissingBean)
 *
 * 3. @ComponentScan
 *    - Scans this package (com.example.orderservice) and sub-packages
 *    - Looks for @Component, @Service, @Repository, @Controller classes
 *    - Registers them as Spring beans in the ApplicationContext
 *    - IMPORTANT: Classes outside this package won't be scanned!
 *
 * HOW SPRING BOOT STARTS:
 * =======================
 * 1. main() calls SpringApplication.run()
 * 2. Spring creates an ApplicationContext (IoC container)
 * 3. Auto-configuration classes run (e.g., WebMvcAutoConfiguration)
 * 4. Component scanning discovers your @Service, @Controller classes
 * 5. Beans are created and dependencies injected
 * 6. Embedded Tomcat starts on port 8080
 * 7. Application is ready to handle requests
 *
 * WHY public static void main()?
 * ==============================
 * - Java entry point - JVM looks for this method
 * - SpringApplication.run() bootstraps Spring context
 * - Returns ConfigurableApplicationContext (can be used for shutdown hooks)
 */
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        // SpringApplication.run() does all the heavy lifting:
        // - Creates ApplicationContext
        // - Registers shutdown hooks
        // - Starts embedded web server
        // - Publishes ApplicationReadyEvent when startup completes
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    /*
     * OPTIONAL: Customize startup behavior
     *
     * SpringApplication app = new SpringApplication(OrderServiceApplication.class);
     * app.setBannerMode(Banner.Mode.OFF);  // Disable Spring Boot banner
     * app.setAdditionalProfiles("dev");     // Activate 'dev' profile
     * app.run(args);
     */
}
