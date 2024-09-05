### Google reinventing SQL

SQL (Structured Query Language) is the go-to language for managing and retrieving data from databases. It's been around for 50 years and is crucial for data engineers, developers, and analysts. However, while SQL is powerful, its syntax can be tricky, especially when you're dealing with complex queries. Recognizing these challenges, Google's researchers are working on a new way to write SQL that could make it easier and more intuitive.


### Traditional SQL, while powerful, has several key disadvantages:

1. *Unintuitive syntax:* The order of clauses (SELECT, FROM, WHERE) doesn't match the logical flow of data processing, making it harder for beginners to understand.

2. *Verbosity:* Complex queries can become long and difficult to read, especially with multiple joins and subqueries.

3. *Steep learning curve:* The syntax and structure of SQL can be intimidating for newcomers, particularly those from non-technical backgrounds.

4. *Limited expressiveness:* Some operations that are simple in procedural languages can be cumbersome to express in SQL.

5. *Readability issues:* As queries grow in complexity, they become harder to read and maintain, especially in collaborative environments.

These limitations have led to various attempts to improve or reinvent SQL.

### Standart SQL examples:
first example:
```
SELECT customer_name, total_amount
FROM orders
WHERE order_date > '2023-01-01'
```
second example:

```
SELECT product_name, AVG(price) as avg_price, SUM(quantity_sold) as total_sold
FROM sales_table
JOIN product_table ON sales_table.product_id = product_table.id
WHERE sale_date BETWEEN '2023-01-01' AND '2023-12-31'
AND category = 'Electronics'
GROUP BY product_name
HAVING total_sold > 100
ORDER BY avg_price DESC
```

This structure can be hard to follow because the `SELECT` clause depends on the `FROM` clause, which actually comes later in the query. Logically, we think about data in a different order: we first decide where the data is coming from (`FROM`), then we filter it (`WHERE`), and finally, we decide what to retrieve (`SELECT`). This reverse order can be confusing, especially for beginners.
Google’s New Approach: The Pipe Syntax
To make SQL more user-friendly, Google’s researchers have introduced a new syntax that uses a "pipe operator" (`|>`). This operator lets you build queries in a way that follows the natural flow of thought. Here’s what the earlier query would look like with Google’s new syntax:

google's new approach for first example:
```
from orders
|> where order_date > '2023-01-01'
|> select customer_name, total_amount
```

google's new approach for second example:
```
FROM sales_table
|> JOIN product_table ON sales_table.product_id = product_table.id
|> WHERE sale_date BETWEEN '2023-01-01' AND '2023-12-31'
AND category = 'Electronics'
|> AGGREGATE 
    product_name,
    AVG(price) AS avg_price,
    SUM(quantity_sold) AS total_sold
GROUP BY product_name
|> HAVING total_sold > 100
|> ORDER BY avg_price DESC
```

This version starts with `FROM` and uses the pipe operator (`|>`) to connect each step, making the query flow more naturally and easier to understand.

### Why it is better?
1. Improved Readability: The pipe syntax follows a more natural, top-to-bottom flow of logic, making queries easier to understand.

2. Logical Order: It aligns with how we typically think about data processing, starting with the data source and ending with the desired output.

3. Modularity: The pipe operator (`|>`) clearly separates each step of the query, making it easier to modify or debug individual parts.

4. Easier for Beginners: The intuitive structure makes SQL more approachable for newcomers and those familiar with modern data analysis tools.

5. Better for Complex Queries: As queries become more intricate, the pipe syntax maintains clarity and readability.

Overall, Google's approach simplifies SQL writing and comprehension, potentially making data analysis more accessible and efficient.

### Conclusion
Google’s new SQL syntax is an exciting development that could make SQL easier to use, especially for beginners. By restructuring how queries are written, Google aims to reduce the confusion that often comes with learning SQL. However, whether this new approach will become the standard is still up in the air. It will depend on whether the wider SQL community and major database systems decide to adopt it.