# Java Core Concepts - Interview Focused

> **Target Audience:** Beginner to intermediate Java developers  
> **Focus:** Collections, Streams, Lambda expressions, Modern Java features  
> **Interview Preparation:** Common questions and patterns

---

## Table of Contents

1. [Collections Framework](#collections-framework)
2. [Java Streams API](#java-streams-api)
3. [Lambda Expressions](#lambda-expressions)
4. [Method References](#method-references)
5. [Optional](#optional)
6. [Common Interview Questions](#common-interview-questions)

---

## Collections Framework

### Overview

The Collections Framework provides interfaces and classes to store and manipulate groups of objects.

```
Collection (interface)
    ├── List (interface) - Ordered, allows duplicates
    │   ├── ArrayList (class)
    │   ├── LinkedList (class)
    │   └── Vector (class)
    ├── Set (interface) - No duplicates
    │   ├── HashSet (class)
    │   ├── LinkedHashSet (class)
    │   └── TreeSet (class)
    └── Queue (interface) - FIFO processing
        ├── PriorityQueue (class)
        └── Deque (interface)

Map (interface) - Key-value pairs, NOT part of Collection hierarchy
    ├── HashMap (class)
    ├── LinkedHashMap (class)
    ├── TreeMap (class)
    ├── Hashtable (class) [legacy]
    └── ConcurrentHashMap (class) [thread-safe]
```

---

### List Interface

**Characteristics:**
- Ordered collection (maintains insertion order)
- Allows duplicate elements
- Allows null values
- Index-based access (0-based)

#### ArrayList vs LinkedList

| Feature | ArrayList | LinkedList |
|---------|-----------|------------|
| **Internal Structure** | Dynamic array | Doubly linked list |
| **Random Access** | O(1) - Fast | O(n) - Slow |
| **Add at end** | O(1) amortized | O(1) |
| **Add at beginning** | O(n) - Slow | O(1) - Fast |
| **Remove from middle** | O(n) | O(n) |
| **Memory** | Less per element | More (stores 2 pointers) |
| **Use When** | Random access, iteration | Frequent insertions at start/end |

#### ArrayList Examples

```java
// Declaration and Initialization
List<String> list = new ArrayList<>();  // Preferred (interface type)
ArrayList<String> list2 = new ArrayList<>();  // Concrete type

// Adding elements
list.add("Apple");
list.add("Banana");
list.add(0, "Mango");  // Insert at index 0

// Accessing elements
String first = list.get(0);  // O(1)
int size = list.size();

// Checking existence
boolean hasApple = list.contains("Apple");  // O(n)

// Iteration
// Method 1: For-each loop
for (String fruit : list) {
    System.out.println(fruit);
}

// Method 2: For loop with index
for (int i = 0; i < list.size(); i++) {
    System.out.println(list.get(i));
}

// Method 3: Iterator
Iterator<String> iter = list.iterator();
while (iter.hasNext()) {
    String item = iter.next();
    // Safe to remove during iteration
    if (item.equals("Banana")) {
        iter.remove();
    }
}

// Method 4: forEach with lambda (Java 8+)
list.forEach(fruit -> System.out.println(fruit));

// Removing elements
list.remove("Apple");  // Remove by object
list.remove(0);  // Remove by index
list.clear();  // Remove all

// Converting to array
String[] array = list.toArray(new String[0]);

// Creating from array
List<String> fromArray = Arrays.asList("A", "B", "C");  // Fixed-size
List<String> mutable = new ArrayList<>(Arrays.asList("A", "B", "C"));  // Mutable

// Java 9+ - List.of() - Immutable
List<String> immutable = List.of("A", "B", "C");  // Cannot add/remove
```

#### Common ArrayList Pitfalls

```java
// ❌ WRONG: ConcurrentModificationException
List<String> list = new ArrayList<>(Arrays.asList("A", "B", "C"));
for (String item : list) {
    if (item.equals("B")) {
        list.remove(item);  // ERROR during iteration
    }
}

// ✅ CORRECT: Use Iterator.remove()
Iterator<String> iter = list.iterator();
while (iter.hasNext()) {
    if (iter.next().equals("B")) {
        iter.remove();
    }
}

// ✅ CORRECT: Use removeIf() (Java 8+)
list.removeIf(item -> item.equals("B"));

// ❌ WRONG: Comparing with ==
String s1 = new String("test");
String s2 = new String("test");
if (s1 == s2) { }  // false - different objects

// ✅ CORRECT: Use .equals()
if (s1.equals(s2)) { }  // true - same content
```

---

### Map Interface

**Characteristics:**
- Stores key-value pairs
- Keys must be unique
- Each key maps to exactly one value
- Not part of Collection interface

#### HashMap

**Internal Structure:**
- Array of buckets (default size: 16)
- Uses hashing to determine bucket index
- Hash collision handling: Chaining (Java 7) → Balanced Tree (Java 8+)

**Time Complexity:**
- `get(key)`: O(1) average, O(log n) worst case (Java 8+)
- `put(key, value)`: O(1) average
- `remove(key)`: O(1) average

```java
// Declaration
Map<String, Integer> map = new HashMap<>();

// Adding key-value pairs
map.put("Alice", 25);
map.put("Bob", 30);
map.put("Charlie", 35);

// Accessing values
Integer age = map.get("Alice");  // 25
Integer notFound = map.get("Dave");  // null

// Check if key exists
if (map.containsKey("Alice")) {
    System.out.println("Found Alice");
}

// Check if value exists (O(n) - slow)
if (map.containsValue(25)) {
    System.out.println("Someone is 25");
}

// Get with default value (Java 8+)
int age2 = map.getOrDefault("Dave", 0);  // Returns 0 if not found

// putIfAbsent - Only add if key doesn't exist
map.putIfAbsent("Alice", 40);  // Won't update, Alice already exists
map.putIfAbsent("Dave", 28);   // Will add Dave

// Updating values
map.put("Alice", 26);  // Replace value

// Removing entries
map.remove("Bob");
map.remove("Alice", 25);  // Remove only if key matches value (Java 8+)

// Size
int count = map.size();

// Iterating over entries
// Method 1: entrySet() - Most efficient
for (Map.Entry<String, Integer> entry : map.entrySet()) {
    System.out.println(entry.getKey() + " -> " + entry.getValue());
}

// Method 2: keySet()
for (String key : map.keySet()) {
    Integer value = map.get(key);
    System.out.println(key + " -> " + value);
}

// Method 3: values()
for (Integer value : map.values()) {
    System.out.println(value);
}

// Method 4: forEach with lambda (Java 8+)
map.forEach((key, value) -> System.out.println(key + " -> " + value));

// Compute operations (Java 8+)
map.compute("Alice", (key, oldValue) -> oldValue + 1);  // Increment

// computeIfPresent - Only if key exists
map.computeIfPresent("Alice", (k, v) -> v + 1);

// computeIfAbsent - Only if key doesn't exist
map.computeIfAbsent("Eve", k -> 22);

// merge - Combine values
map.merge("Alice", 5, (oldValue, newValue) -> oldValue + newValue);
```

#### HashMap vs ConcurrentHashMap

| Feature | HashMap | ConcurrentHashMap |
|---------|---------|-------------------|
| **Thread Safety** | Not thread-safe | Thread-safe |
| **Null Keys** | Allows 1 null key | Does NOT allow null |
| **Null Values** | Allows null values | Does NOT allow null |
| **Performance** | Faster (single-threaded) | Slightly slower (locking overhead) |
| **Iteration** | Fail-fast | Fail-safe (weakly consistent) |
| **Use Case** | Single-threaded apps | Multi-threaded apps |

```java
// ConcurrentHashMap - Thread-safe without locking entire map
Map<String, Order> orderStore = new ConcurrentHashMap<>();

// Safe for concurrent access
orderStore.put("order-1", order1);  // Thread 1
Order o = orderStore.get("order-1");  // Thread 2 - Safe

// Atomic operations
orderStore.computeIfAbsent("order-2", key -> createOrder(key));
```

#### When to Use Which Map

```java
// HashMap - Single-threaded, need null keys/values
Map<String, String> config = new HashMap<>();

// ConcurrentHashMap - Multi-threaded, no null keys/values
Map<String, Order> orderCache = new ConcurrentHashMap<>();

// LinkedHashMap - Preserve insertion order
Map<String, Integer> orderedMap = new LinkedHashMap<>();

// TreeMap - Sorted by keys
Map<String, Integer> sortedMap = new TreeMap<>();
```

---

## Java Streams API

**What is a Stream?**
- A sequence of elements supporting sequential and parallel operations
- NOT a data structure (doesn't store elements)
- Functional-style operations on collections
- Lazy evaluation (intermediate operations don't execute until terminal operation)

**Stream Pipeline:**
```
Source → Intermediate Operations (0 or more) → Terminal Operation
```

### Creating Streams

```java
// From Collections
List<String> list = Arrays.asList("A", "B", "C");
Stream<String> stream1 = list.stream();

// From Arrays
String[] array = {"A", "B", "C"};
Stream<String> stream2 = Arrays.stream(array);

// From values
Stream<String> stream3 = Stream.of("A", "B", "C");

// From IntStream (primitive streams)
IntStream numbers = IntStream.range(1, 10);  // 1 to 9
IntStream numbers2 = IntStream.rangeClosed(1, 10);  // 1 to 10

// Infinite streams
Stream<Integer> infinite = Stream.iterate(0, n -> n + 2);  // 0, 2, 4, 6...
Stream<Double> random = Stream.generate(Math::random);
```

### Intermediate Operations (Lazy)

**Characteristics:**
- Return a Stream
- Do NOT execute until terminal operation is called
- Can be chained

#### filter()

```java
// Filter elements based on predicate
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6);

List<Integer> evens = numbers.stream()
    .filter(n -> n % 2 == 0)  // Keep only even numbers
    .collect(Collectors.toList());  // [2, 4, 6]

// Multiple filters (chained)
List<Integer> result = numbers.stream()
    .filter(n -> n > 2)
    .filter(n -> n < 6)
    .collect(Collectors.toList());  // [3, 4, 5]
```

#### map()

```java
// Transform each element
List<String> names = Arrays.asList("alice", "bob", "charlie");

List<String> upperNames = names.stream()
    .map(String::toUpperCase)  // Transform to uppercase
    .collect(Collectors.toList());  // ["ALICE", "BOB", "CHARLIE"]

// Map to different type
List<Integer> nameLengths = names.stream()
    .map(String::length)
    .collect(Collectors.toList());  // [5, 3, 7]

// Complex mapping (DTO to Domain)
List<OrderItemRequest> dtos = getOrderItemRequests();
List<OrderItem> items = dtos.stream()
    .map(dto -> new OrderItem(
        dto.getProductId(),
        dto.getProductName(),
        dto.getQuantity(),
        dto.getPrice()
    ))
    .collect(Collectors.toList());
```

#### flatMap()

```java
// Flatten nested structures
List<List<String>> nestedList = Arrays.asList(
    Arrays.asList("A", "B"),
    Arrays.asList("C", "D"),
    Arrays.asList("E", "F")
);

// Without flatMap (doesn't work)
List<String> flat = nestedList.stream()
    .map(list -> list)  // Stream<List<String>> - WRONG
    .collect(Collectors.toList());

// With flatMap
List<String> flat = nestedList.stream()
    .flatMap(list -> list.stream())  // Flatten to Stream<String>
    .collect(Collectors.toList());  // ["A", "B", "C", "D", "E", "F"]

// Real-world example: Get all items from all orders
List<Order> orders = getAllOrders();
List<OrderItem> allItems = orders.stream()
    .flatMap(order -> order.getItems().stream())
    .collect(Collectors.toList());
```

#### distinct()

```java
// Remove duplicates
List<Integer> numbers = Arrays.asList(1, 2, 2, 3, 3, 4);
List<Integer> unique = numbers.stream()
    .distinct()
    .collect(Collectors.toList());  // [1, 2, 3, 4]
```

#### sorted()

```java
List<Integer> numbers = Arrays.asList(5, 2, 8, 1, 9);

// Natural order
List<Integer> sorted = numbers.stream()
    .sorted()
    .collect(Collectors.toList());  // [1, 2, 5, 8, 9]

// Custom comparator
List<Integer> reversed = numbers.stream()
    .sorted((a, b) -> b - a)  // Descending
    .collect(Collectors.toList());  // [9, 8, 5, 2, 1]

// Sorting objects
List<Order> orders = getOrders();
List<Order> sortedByAmount = orders.stream()
    .sorted((o1, o2) -> o1.getTotalAmount().compareTo(o2.getTotalAmount()))
    .collect(Collectors.toList());

// Using Comparator methods (cleaner)
List<Order> sortedByAmount2 = orders.stream()
    .sorted(Comparator.comparing(Order::getTotalAmount))
    .collect(Collectors.toList());

// Reverse order
List<Order> sortedDesc = orders.stream()
    .sorted(Comparator.comparing(Order::getTotalAmount).reversed())
    .collect(Collectors.toList());
```

#### limit() and skip()

```java
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

// Take first 5
List<Integer> first5 = numbers.stream()
    .limit(5)
    .collect(Collectors.toList());  // [1, 2, 3, 4, 5]

// Skip first 5, take rest
List<Integer> skipFirst5 = numbers.stream()
    .skip(5)
    .collect(Collectors.toList());  // [6, 7, 8, 9, 10]

// Pagination: skip 10, take 5
List<Integer> page2 = numbers.stream()
    .skip(10)
    .limit(5)
    .collect(Collectors.toList());
```

### Terminal Operations (Eager)

**Characteristics:**
- Trigger execution of the stream pipeline
- Return non-stream result (or void)
- Can only be called once on a stream

#### collect()

```java
// Most versatile terminal operation
List<String> names = Arrays.asList("Alice", "Bob", "Charlie");

// To List
List<String> list = names.stream()
    .collect(Collectors.toList());

// To Set
Set<String> set = names.stream()
    .collect(Collectors.toSet());

// To Map
List<Order> orders = getOrders();
Map<String, Order> orderMap = orders.stream()
    .collect(Collectors.toMap(
        Order::getOrderId,  // Key mapper
        order -> order       // Value mapper (identity)
    ));

// Grouping
Map<OrderStatus, List<Order>> ordersByStatus = orders.stream()
    .collect(Collectors.groupingBy(Order::getStatus));

// Counting
Map<OrderStatus, Long> countByStatus = orders.stream()
    .collect(Collectors.groupingBy(
        Order::getStatus,
        Collectors.counting()
    ));

// Joining strings
String joined = names.stream()
    .collect(Collectors.joining(", "));  // "Alice, Bob, Charlie"

String withPrefix = names.stream()
    .collect(Collectors.joining(", ", "[", "]"));  // "[Alice, Bob, Charlie]"
```

#### forEach()

```java
// Execute action for each element (void)
List<String> names = Arrays.asList("Alice", "Bob");
names.stream()
    .forEach(name -> System.out.println(name));

// Method reference
names.forEach(System.out::println);  // Simpler

// ⚠️ Note: Cannot break out of forEach
// Use for-loop if you need early exit
```

#### count()

```java
long count = numbers.stream()
    .filter(n -> n > 5)
    .count();
```

#### anyMatch(), allMatch(), noneMatch()

```java
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

// Check if any element matches
boolean hasEven = numbers.stream()
    .anyMatch(n -> n % 2 == 0);  // true

// Check if all elements match
boolean allPositive = numbers.stream()
    .allMatch(n -> n > 0);  // true

// Check if no elements match
boolean noNegative = numbers.stream()
    .noneMatch(n -> n < 0);  // true
```

#### findFirst(), findAny()

```java
// Find first matching element
Optional<Integer> first = numbers.stream()
    .filter(n -> n > 3)
    .findFirst();  // Optional[4]

// Find any matching element (useful in parallel streams)
Optional<Integer> any = numbers.stream()
    .filter(n -> n > 3)
    .findAny();  // Optional[4] or Optional[5]
```

#### reduce()

```java
// Reduce to single value
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

// Sum
int sum = numbers.stream()
    .reduce(0, (a, b) -> a + b);  // 15

// Using Integer::sum
int sum2 = numbers.stream()
    .reduce(0, Integer::sum);

// Max
Optional<Integer> max = numbers.stream()
    .reduce((a, b) -> a > b ? a : b);

// Using Integer::max
Optional<Integer> max2 = numbers.stream()
    .reduce(Integer::max);
```

#### min(), max(), sum(), average()

```java
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

// Min
Optional<Integer> min = numbers.stream()
    .min(Integer::compareTo);  // Optional[1]

// Max
Optional<Integer> max = numbers.stream()
    .max(Integer::compareTo);  // Optional[5]

// For IntStream (primitive stream)
IntStream intStream = IntStream.of(1, 2, 3, 4, 5);

int sum = intStream.sum();  // 15
OptionalDouble avg = IntStream.of(1, 2, 3, 4, 5).average();  // 3.0
OptionalInt max = IntStream.of(1, 2, 3, 4, 5).max();  // 5
```

### Real-World Stream Examples

```java
// Example 1: Get total amount of all PENDING orders
List<Order> orders = getAllOrders();

BigDecimal totalPending = orders.stream()
    .filter(order -> order.getStatus() == OrderStatus.PENDING)
    .map(Order::getTotalAmount)
    .reduce(BigDecimal.ZERO, BigDecimal::add);

// Example 2: Get unique product IDs from all orders
Set<String> productIds = orders.stream()
    .flatMap(order -> order.getItems().stream())
    .map(OrderItem::getProductId)
    .collect(Collectors.toSet());

// Example 3: Group orders by customer
Map<String, List<Order>> ordersByCustomer = orders.stream()
    .collect(Collectors.groupingBy(Order::getCustomerId));

// Example 4: Convert Order list to OrderResponse list
List<OrderResponse> responses = orders.stream()
    .map(order -> {
        OrderResponse response = new OrderResponse();
        response.setOrderId(order.getOrderId());
        response.setCustomerId(order.getCustomerId());
        response.setTotalAmount(order.getTotalAmount());
        response.setStatus(order.getStatus().name());
        return response;
    })
    .collect(Collectors.toList());

// Example 5: Find orders with total > 1000
List<Order> highValueOrders = orders.stream()
    .filter(order -> order.getTotalAmount().compareTo(new BigDecimal("1000")) > 0)
    .sorted(Comparator.comparing(Order::getTotalAmount).reversed())
    .collect(Collectors.toList());
```

---

## Lambda Expressions

**What is a Lambda?**
- Anonymous function (function without a name)
- Concise way to represent functional interfaces
- Introduced in Java 8

**Syntax:**
```
(parameters) -> expression
(parameters) -> { statements; }
```

### Lambda Examples

```java
// No parameters
Runnable r = () -> System.out.println("Hello");

// Single parameter (parentheses optional)
Consumer<String> c1 = s -> System.out.println(s);
Consumer<String> c2 = (s) -> System.out.println(s);

// Multiple parameters
BiFunction<Integer, Integer, Integer> add = (a, b) -> a + b;

// Multiple statements (need braces)
Consumer<String> c3 = s -> {
    String upper = s.toUpperCase();
    System.out.println(upper);
};

// Returning value (explicit return needed with braces)
Function<Integer, Integer> square = x -> {
    int result = x * x;
    return result;
};

// Single expression (implicit return)
Function<Integer, Integer> square2 = x -> x * x;
```

### Functional Interfaces

**Definition:** Interface with exactly ONE abstract method

**Common Functional Interfaces:**

```java
// Predicate<T> - boolean test(T t)
Predicate<Integer> isEven = n -> n % 2 == 0;
boolean result = isEven.test(4);  // true

// Function<T, R> - R apply(T t)
Function<String, Integer> length = s -> s.length();
int len = length.apply("Hello");  // 5

// Consumer<T> - void accept(T t)
Consumer<String> printer = s -> System.out.println(s);
printer.accept("Hello");

// Supplier<T> - T get()
Supplier<Double> randomSupplier = () -> Math.random();
double rand = randomSupplier.get();

// BiFunction<T, U, R> - R apply(T t, U u)
BiFunction<Integer, Integer, Integer> multiply = (a, b) -> a * b;
int product = multiply.apply(3, 4);  // 12

// Comparator<T> - int compare(T o1, T o2)
Comparator<String> byLength = (s1, s2) -> s1.length() - s2.length();
```

### Custom Functional Interface

```java
@FunctionalInterface
public interface OrderValidator {
    boolean validate(Order order);
}

// Usage
OrderValidator validator = order -> {
    return order.getTotalAmount().compareTo(BigDecimal.ZERO) > 0
        && order.getItems() != null
        && !order.getItems().isEmpty();
};

boolean isValid = validator.validate(order);
```

---

## Method References

**What is a Method Reference?**
- Shorthand notation for lambda expressions
- Refers to an existing method by name
- Syntax: `ClassName::methodName`

### Types of Method References

#### 1. Static Method Reference

```java
// Lambda
Function<String, Integer> parser1 = s -> Integer.parseInt(s);

// Method reference
Function<String, Integer> parser2 = Integer::parseInt;

// Usage
int num = parser2.apply("123");  // 123
```

#### 2. Instance Method Reference (on specific object)

```java
String prefix = "Order: ";

// Lambda
Function<String, String> addPrefix1 = s -> prefix.concat(s);

// Method reference
Function<String, String> addPrefix2 = prefix::concat;
```

#### 3. Instance Method Reference (on parameter)

```java
// Lambda
Function<String, Integer> length1 = s -> s.length();

// Method reference
Function<String, Integer> length2 = String::length;

// Usage in streams
List<String> names = Arrays.asList("Alice", "Bob", "Charlie");
List<Integer> lengths = names.stream()
    .map(String::length)  // Same as: s -> s.length()
    .collect(Collectors.toList());
```

#### 4. Constructor Reference

```java
// Lambda
Supplier<ArrayList<String>> listSupplier1 = () -> new ArrayList<>();

// Constructor reference
Supplier<ArrayList<String>> listSupplier2 = ArrayList::new;

// With parameters
Function<String, BigDecimal> bdCreator = BigDecimal::new;
BigDecimal bd = bdCreator.apply("99.99");

// Usage in streams
List<String> strings = Arrays.asList("1.5", "2.5", "3.5");
List<BigDecimal> decimals = strings.stream()
    .map(BigDecimal::new)
    .collect(Collectors.toList());
```

### Common Method Reference Examples

```java
// System.out::println
list.forEach(System.out::println);  // Same as: item -> System.out.println(item)

// String::toUpperCase
list.stream().map(String::toUpperCase);  // Same as: s -> s.toUpperCase()

// Order::getOrderId
orders.stream().map(Order::getOrderId);  // Same as: order -> order.getOrderId()

// Comparator.comparing with method reference
orders.stream()
    .sorted(Comparator.comparing(Order::getTotalAmount));
```

---

## Optional

**What is Optional?**
- Container object that may or may not contain a non-null value
- Introduced in Java 8 to avoid `NullPointerException`
- Forces explicit handling of null cases

### Creating Optionals

```java
// Empty Optional
Optional<String> empty = Optional.empty();

// Optional with value
Optional<String> opt1 = Optional.of("Hello");  // Throws NPE if null

// Optional that might be null
Optional<String> opt2 = Optional.ofNullable(getValue());  // Returns empty if null
```

### Checking for Value

```java
Optional<String> opt = Optional.ofNullable(getValue());

// Check if present
if (opt.isPresent()) {
    String value = opt.get();
    System.out.println(value);
}

// isEmpty (Java 11+)
if (opt.isEmpty()) {
    System.out.println("No value");
}
```

### Getting Values

```java
// get() - Throws NoSuchElementException if empty
String value = opt.get();  // ⚠️ Avoid, use alternatives

// orElse() - Return default value if empty
String value = opt.orElse("default");

// orElseGet() - Supplier called only if empty (lazy)
String value = opt.orElseGet(() -> computeDefault());

// orElseThrow() - Throw exception if empty
String value = opt.orElseThrow(() -> new OrderNotFoundException());

// orElseThrow() with default exception (Java 10+)
String value = opt.orElseThrow();  // Throws NoSuchElementException
```

### Transforming Optionals

```java
Optional<String> opt = Optional.of("hello");

// map() - Transform value if present
Optional<Integer> length = opt.map(String::length);  // Optional[5]

// flatMap() - Avoid nested Optionals
Optional<String> opt1 = Optional.of("hello");
Optional<String> result = opt1.flatMap(s -> Optional.of(s.toUpperCase()));

// filter() - Keep value only if matches predicate
Optional<String> filtered = opt.filter(s -> s.length() > 3);
```

### Consuming Values

```java
Optional<String> opt = Optional.of("hello");

// ifPresent() - Execute if value present
opt.ifPresent(value -> System.out.println(value));

// ifPresentOrElse() - Execute different actions (Java 9+)
opt.ifPresentOrElse(
    value -> System.out.println(value),
    () -> System.out.println("No value")
);
```

### Real-World Optional Usage

```java
// Bad: Using null
public Order getOrderById(String id) {
    Order order = orderStore.get(id);
    if (order == null) {
        throw new IllegalArgumentException("Order not found");
    }
    return order;
}

// Better: Using Optional
public Optional<Order> getOrderById(String id) {
    return Optional.ofNullable(orderStore.get(id));
}

// Usage
Optional<Order> orderOpt = service.getOrderById("123");

// Pattern 1: orElseThrow
Order order = orderOpt.orElseThrow(
    () -> new OrderNotFoundException("Order not found: 123")
);

// Pattern 2: ifPresent
orderOpt.ifPresent(order -> {
    processOrder(order);
});

// Pattern 3: map + orElse
BigDecimal total = orderOpt
    .map(Order::getTotalAmount)
    .orElse(BigDecimal.ZERO);

// Pattern 4: Stream integration
List<Order> orders = getOptionalOrders().stream()
    .filter(Optional::isPresent)
    .map(Optional::get)
    .collect(Collectors.toList());

// Or using flatMap (cleaner)
List<Order> orders = getOptionalOrders().stream()
    .flatMap(Optional::stream)  // Java 9+
    .collect(Collectors.toList());
```

---

## Common Interview Questions

### 1. ArrayList vs LinkedList

**Q:** When would you use ArrayList over LinkedList?

**A:** 
- Use **ArrayList** when you need frequent random access (get by index) or when iterating through all elements
- Use **LinkedList** when you frequently insert/remove at the beginning or middle of the list
- ArrayList is default choice; LinkedList is rarely used in practice

### 2. HashMap Internals

**Q:** How does HashMap work internally?

**A:**
1. HashMap uses array of buckets (default size 16)
2. Uses `hashCode()` of key to determine bucket index
3. If multiple keys hash to same bucket (collision), uses chaining (LinkedList in Java 7, balanced tree in Java 8+)
4. Load factor (0.75) triggers resizing when 75% full
5. Resizing doubles the array size and rehashes all entries

**Q:** What happens when two keys have same hashCode?

**A:** Hash collision. HashMap stores multiple entries in same bucket using a linked list (or tree if >8 entries)

### 3. ConcurrentHashMap

**Q:** Why use ConcurrentHashMap over HashMap?

**A:**
- **Thread safety**: ConcurrentHashMap is thread-safe, HashMap is not
- **Performance**: ConcurrentHashMap uses segment-level locking (Java 7) or CAS operations (Java 8+), allowing concurrent reads/writes
- **No null keys/values**: ConcurrentHashMap doesn't allow null (HashMap allows one null key)

**Q:** When NOT to use ConcurrentHashMap?

**A:** Single-threaded applications. HashMap is faster due to no locking overhead.

### 4. Java Streams

**Q:** What's the difference between map() and flatMap()?

**A:**
- **map()**: One-to-one transformation (input element → output element)
- **flatMap()**: One-to-many transformation, then flattens (input element → Stream of elements → flattened)

```java
// map: List<String> → List<Integer>
List<Integer> lengths = names.stream().map(String::length).collect(toList());

// flatMap: List<List<String>> → List<String>
List<String> flat = nestedLists.stream().flatMap(List::stream).collect(toList());
```

**Q:** What's lazy evaluation in streams?

**A:** Intermediate operations (filter, map) don't execute until a terminal operation (collect, forEach) is called.

```java
Stream<Integer> stream = list.stream()
    .filter(n -> n > 5)  // Not executed yet
    .map(n -> n * 2);     // Not executed yet

List<Integer> result = stream.collect(Collectors.toList());  // NOW executes
```

### 5. Lambda Expressions

**Q:** What is a functional interface?

**A:** Interface with exactly ONE abstract method. Examples: `Runnable`, `Comparator`, `Predicate`, `Function`

**Q:** Can you modify external variables inside lambda?

**A:** Only if they're effectively final (not reassigned after initialization).

```java
int x = 10;
// x = 20;  // If uncommented, compilation error in lambda below

Consumer<Integer> c = n -> System.out.println(n + x);  // OK, x is effectively final
```

### 6. Optional

**Q:** Why use Optional instead of null?

**A:**
- **Explicit**: Forces caller to handle null case
- **Prevents NPE**: Compilation error if you forget to check
- **Functional style**: Works well with streams and lambdas
- **API design**: Method signature makes it clear value might be absent

**Q:** Should you use Optional for fields?

**A:** **NO**. Optional is designed for return types, not fields or parameters. It's not serializable and adds overhead.

```java
// ❌ BAD
public class Order {
    private Optional<String> notes;  // Don't do this
}

// ✅ GOOD
public class Order {
    private String notes;  // Can be null
    
    public Optional<String> getNotes() {
        return Optional.ofNullable(notes);
    }
}
```

### 7. Stream vs Loop

**Q:** When to use streams vs traditional loops?

**A:**
- Use **streams** when:
  - Transforming collections (map, filter)
  - Functional-style code is clearer
  - Potential for parallel processing
- Use **loops** when:
  - Need to break/continue based on complex conditions
  - Throwing checked exceptions
  - Performance-critical (streams have overhead)

### 8. Immutable Collections

**Q:** How to create immutable lists?

**A:**
```java
// Java 9+
List<String> immutable = List.of("A", "B", "C");  // Cannot add/remove

// Pre-Java 9
List<String> immutable = Collections.unmodifiableList(new ArrayList<>(list));
```

---

## Performance Tips

### 1. ArrayList Capacity

```java
// ❌ BAD: Default capacity (10), resizes multiple times
List<String> list = new ArrayList<>();
for (int i = 0; i < 1000; i++) {
    list.add("item");
}

// ✅ GOOD: Pre-allocate capacity
List<String> list = new ArrayList<>(1000);
for (int i = 0; i < 1000; i++) {
    list.add("item");
}
```

### 2. HashMap Initial Capacity

```java
// ❌ BAD: Default capacity (16), resizes multiple times
Map<String, String> map = new HashMap<>();

// ✅ GOOD: Pre-allocate capacity
Map<String, String> map = new HashMap<>(100);  // For ~75 entries
```

### 3. Stream Performance

```java
// ❌ BAD: Multiple passes
List<Order> filtered = orders.stream()
    .filter(o -> o.getStatus() == OrderStatus.PENDING)
    .collect(toList());

List<OrderResponse> responses = filtered.stream()
    .map(this::mapToResponse)
    .collect(toList());

// ✅ GOOD: Single pass
List<OrderResponse> responses = orders.stream()
    .filter(o -> o.getStatus() == OrderStatus.PENDING)
    .map(this::mapToResponse)
    .collect(toList());
```

### 4. Parallel Streams

```java
// Use for large datasets (>10,000 elements) with CPU-intensive operations
List<Order> orders = getHugeOrderList();

List<OrderResponse> responses = orders.parallelStream()  // Parallel
    .filter(o -> o.getStatus() == OrderStatus.PENDING)
    .map(this::expensiveTransformation)
    .collect(toList());

// ⚠️ Don't use parallel streams for:
// - Small datasets (<1000 elements)
// - I/O operations (database, network)
// - When order matters
```

---

## Summary Cheat Sheet

```java
// LISTS
List<String> list = new ArrayList<>();
list.add("A");
String first = list.get(0);
list.remove(0);
list.size();

// MAPS
Map<String, Integer> map = new HashMap<>();
map.put("key", 123);
Integer val = map.get("key");
map.remove("key");
map.containsKey("key");

// STREAMS
List<Integer> result = numbers.stream()
    .filter(n -> n > 5)           // Keep elements
    .map(n -> n * 2)              // Transform
    .sorted()                      // Sort
    .distinct()                    // Remove duplicates
    .limit(10)                     // Take first 10
    .collect(Collectors.toList()); // Collect to list

// LAMBDA
Function<Integer, Integer> square = x -> x * x;
Predicate<Integer> isEven = n -> n % 2 == 0;
Consumer<String> print = s -> System.out.println(s);

// METHOD REFERENCE
list.forEach(System.out::println);
list.stream().map(String::toUpperCase);
list.stream().map(Integer::parseInt);

// OPTIONAL
Optional<String> opt = Optional.ofNullable(value);
String val = opt.orElse("default");
opt.ifPresent(v -> System.out.println(v));
String val = opt.orElseThrow(() -> new Exception());
```

---

**Next Steps:**
1. Practice stream operations on LeetCode/HackerRank
2. Implement your own functional interfaces
3. Refactor existing code to use streams
4. Learn parallel streams for performance
5. Study Java 9+ features (var, sealed classes, records)
