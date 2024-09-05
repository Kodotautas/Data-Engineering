SQL (Structured Query Language) is the go-to language for managing and retrieving data from databases. It's been around for decades and is crucial for data engineers, developers, and analysts. However, while SQL is powerful, its syntax can be tricky, especially when you're dealing with complex queries. Recognizing these challenges, Google's researchers are working on a new way to write SQL that could make it easier and more intuitive.


Photo by Arthur Osipyan on Unsplash
What’s Wrong with Traditional SQL?
Traditional SQL can be confusing, particularly because of the way queries are structured. Normally, a SQL query starts with the `SELECT` clause, like this:
SELECT component_id, COUNT(*)
FROM ticketing_system_table
WHERE assignee_user.email = 'username@email.com'
AND status IN (’NEW’, 'ASSIGNED’, 'ACCEPTED’)
GROUP BY component_id
ORDER BY component_id DESC; 

This structure can be hard to follow because the `SELECT` clause depends on the `FROM` clause, which actually comes later in the query. Logically, we think about data in a different order: we first decide where the data is coming from (`FROM`), then we filter it (`WHERE`), and finally, we decide what to retrieve (`SELECT`). This reverse order can be confusing, especially for beginners.
Google’s New Approach: The Pipe Syntax
To make SQL more user-friendly, Google’s researchers have introduced a new syntax that uses a "pipe operator" (`|>`). This operator lets you build queries in a way that follows the natural flow of thought. Here’s what the earlier query would look like with Google’s new syntax:
FROM ticketing_system_table
|> WHERE assignee_user.email = 'username@email.com'
AND status IN (’NEW’, 'ASSIGNED’, 'ACCEPTED’)
|> AGGREGATE COUNT(*)
GROUP AND ORDER BY component_id DESC;

This version starts with `FROM`, making it easier to follow. Each step of the query is connected by the pipe operator (`|>`), which means "take the result from the previous step and apply the next operation." This way, the query reads more like a set of instructions that follow the order we naturally think in.
Why This is Important
Easier to Read: This new syntax makes SQL queries easier to read and understand because the order of operations follows a logical flow.
Fewer Mistakes: Since the query structure is more straightforward, it’s less likely that you’ll make mistakes when writing complex queries.
Better for Maintenance: Easier-to-read queries are also easier to maintain. When you or someone else needs to update the query later, it’s quicker to understand what’s going on.
Modern Programming Style: The pipe syntax is similar to patterns used in modern programming languages, making it more familiar to developers who already use these languages.
What People Think About It
Google has started using this new syntax internally, especially in their GoogleSQL dialect, which is used in services like BigQuery. As of August 2024, around 1,600 users at Google are already using this new way to write SQL, and they seem to like it.
However, not everyone is convinced. Dr. Richard Hipp, the creator of SQLite, is one of the skeptics. He agrees that starting with `FROM` could be more intuitive, but he’s not sure the pipe syntax is necessary. He thinks the traditional SQL syntax still works fine and doesn’t see a big enough benefit to justify the change.
What Could Happen Next?
It’s still unclear whether this new SQL syntax will catch on widely. A few things could influence its success:

Community Support: If developers, database administrators, and educators like the new syntax, it could become more popular.
Compatibility with Other Systems: For the new syntax to really take off, it would need to be supported by other popular database systems like PostgreSQL and MySQL.
Tool Support: Developers would need their tools, like SQL editors and integrated development environments (IDEs), to support this new syntax with features like syntax highlighting and error checking.
Conclusion
Google’s new SQL syntax is an exciting development that could make SQL easier to use, especially for beginners. By restructuring how queries are written, Google aims to reduce the confusion that often comes with learning SQL. However, whether this new approach will become the standard is still up in the air. It will depend on whether the wider SQL community and major database systems decide to adopt it.