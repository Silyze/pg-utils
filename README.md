# pg-utils

**pg-utils** is a Node.js library designed to simplify interactions with PostgreSQL databases by providing convenient functions for common tasks such as inserting, updating, querying, and error handling. This package utilizes the popular `pg` library for database connections.

## Installation

You can install **pg-utils** via npm:

```bash
npm install pg-utils
```

## Usage

### Importing

```javascript
import { Pool } from 'pg';
import { insert, update, has, getValue } from 'pg-utils';
```

### Connecting to PostgreSQL

Before using any functions, make sure you have established a connection to your PostgreSQL database using `pg.Pool`.

```javascript
const pool = new Pool({
  // Your PostgreSQL connection configuration
});
```

### Inserting Data

```javascript
const tempData = { /* Data to be inserted */ };
const tableName = 'your_table_name';

// Insert data into the specified table
const result = await insert(pool, tempData, tableName);

// Optionally, specify columns to return after insertion
const columnsToReturn = ['id', 'name'];
const resultWithReturn = await insert(pool, tempData, tableName, columnsToReturn);
```

### Updating Data

```javascript
const updateData = { /* Updated data */ };
const tableName = 'your_table_name';
const whereClause = [['id', 123]]; // Example WHERE clause

await update(pool, updateData, tableName, whereClause);
```

### Querying Data

```javascript
const query = 'SELECT * FROM your_table WHERE condition = true';

// Check if any data exists based on the query
const exists = await has(pool, query);

// Get data based on the query
const data = await getValue(pool, query);
```

### Error Handling

The package provides custom error handling with `ThrowError` class, which extends `Error`.

```javascript
try {
  // Your code using pg-utils functions
} catch (error) {
  if (error instanceof ThrowError) {
    console.error('Query execution failed:', error.query);
  } else {
    console.error('An unexpected error occurred:', error);
  }
}
```

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).