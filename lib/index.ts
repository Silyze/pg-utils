import type { Pool } from "pg";

export class ThrowError extends Error {
    constructor(public readonly query: string) {
        super(`Query has failed: ${query}`)
    }
}

export async function insert(
    client: Pool,
    temp: any,
    tableName: string,
    returning: string[] | null = null,
    num: number = 0
  ): Promise<any> {
    // Remove keys with null or undefined values.
    for (const key of Object.keys(temp)) {
      if (temp[key] == null) {
        delete temp[key];
      }
    }
  
    // If nothing remains to insert, return null.
    const keys = Object.keys(temp);
    if (keys.length === 0) {
      return null;
    }
  
    // Build parameterized query parts.
    const values = keys.map((key) => temp[key]);
    const placeholders = keys.map((_, index) => `$${index + 1}`).join(", ");
    const columns = keys.join(", ");
  
    // Build query text with optional RETURNING clause.
    const returningClause = returning ? ` RETURNING ${returning.join(", ")}` : "";
    const queryText = `INSERT INTO ${tableName} (${columns}) VALUES (${placeholders})${returningClause}`;
  
    const conn = await client.connect();
  
    try {
      const res = await conn.query(queryText, values);
      return res.rows;
    } catch (error) {
      num++;
      if (num >= 2) {
        throw new ThrowError(queryText);
      }
      return insert(client, temp, tableName, returning, num);
    } finally {
      conn.release();
    }
}  

export async function update(
    client: Pool,
    temp: any,
    table: string,
    where: any[][],
    num: number = 0
): Promise<void> {
    // Remove keys with null or undefined values.
    Object.keys(temp).forEach((key) => {
        if (temp[key] == null) {
            delete temp[key];
        }
    });
  
    // If there's nothing to update, exit early.
    if (Object.keys(temp).length === 0) {
        return;
    }
  
    // Build the SET clause using parameterized placeholders.
    const setKeys = Object.keys(temp);
    const setClauses = setKeys.map((key, index) => `${key} = $${index + 1}`);
    const setValues = setKeys.map(key => temp[key]);
  
    // Build the WHERE clause.
    // Assumes each element in `where` is an array of [column, value].
    const whereClauses = where.map((condition, i) => {
        return `${condition[0]} = $${setValues.length + i + 1}`;
    });
    const whereValues = where.map(condition => condition[1]);
  
    // Combine SET and WHERE values.
    const queryText = `UPDATE ${table} SET ${setClauses.join(", ")} WHERE ${whereClauses.join(" AND ")}`;
    const queryValues = [...setValues, ...whereValues];
  
    const conn = await client.connect();
  
    try {
        await conn.query(queryText, queryValues);
    } catch (error) {
        num++;
        if (num >= 2) {
            throw new ThrowError(queryText);
        }
        return update(client, temp, table, where, num);
    } finally {
        conn.release();
    }
}

export async function has(client: Pool, query: string, num: number = 0): Promise<boolean> {
    const conn = await client.connect();

    try {
        const res = await conn.query(query);
        return res.rows.length > 0;
    } catch (error) {
        num++;
        if (num >= 2) {
            throw new ThrowError(query);
        }
        return has(client, query, num); // Ensure the recursive call returns a value
    } finally {
        conn.release(); // Always release the connection
    }
}


export async function getValue(client: Pool, query: string, num: number = 0): Promise<any> {
    const conn = await client.connect();
  
    try {
        const res = await conn.query(query);
        return res.rows;
    } catch (error) {
        num++;
        if (num >= 2) {
            throw new ThrowError(query);
        }
        return getValue(client, query, num); // Ensure the recursive call returns a value
    } finally {
        conn.release(); // Always release the connection
    }
}
