import type { Pool } from "pg";

export class ThrowError extends Error {
    constructor(public readonly query: string) {
        super(`Query has failed: ${query}`)
    }
}

export async function insert (client: Pool, temp: any, tableName: string, returning: string[] | null = null, num: number = 0): Promise<any> {
  for (let key of Object.keys(temp)) {
      if (temp[key] == null) {
          delete temp[key];
      }
  }

  if (Object.keys(temp).length == 0) {
      return null;
  }

  let keyString: string = "";
  let valueString: string = "";
  for (let key of Object.keys(temp)) {
      keyString += `${key},`;

      if (typeof temp[key] == "number" || typeof temp[key] == "boolean") {
          valueString += `${temp[key]},`;
      } else if (typeof temp[key] == "string") {
          if (temp[key].includes("{")) {
              valueString += `'${temp[key].replaceAll("'", "''")}',`;
          } else {
              valueString += `'${temp[key].replaceAll("'", "''").replaceAll("\"", "''")}',`;
          }
      } else if (typeof temp[key] == "object") {
          valueString += `'{"${temp[key].join("\", \"").replaceAll("'", "''")}"}',`;
      }
  }

  keyString = keyString.slice(0, -1);
  valueString = valueString.slice(0, -1);

  const conn = await client.connect();

  let result = 0;

  if (returning == null) {
      await conn.query(`INSERT INTO ${tableName} (${keyString})
                            VALUES (${valueString})`)
          .then(async (res: any) => {
              result = res.rows;
          })
          .catch(async (e: any) => {
              num++;
              if (num == 2) {
                throw new ThrowError(`INSERT INTO ${tableName} (${keyString})
                VALUES (${valueString})`);
              }
              await insert(client, temp, tableName, returning, num);
          });
  } else {
      const ret = returning.toString();
      await conn.query(`INSERT INTO ${tableName} (${keyString})
                            VALUES (${valueString})
                            RETURNING ${ret}`)
          .then(async (res: any) => {
              result = res.rows;
          })
          .catch(async (e: any) => {
              num++;
              if (num == 2) {
                throw new ThrowError(`INSERT INTO ${tableName} (${keyString})
                VALUES (${valueString})`);
              }
              await insert(client, temp, tableName, returning, num);
          });
  }

  conn.release();

  return result;
}

export async function update (client: Pool, temp: any, table: string, where: any[][], num: number = 0): Promise<void> {
    for (let key of Object.keys(temp)) {
        if (temp[key] == null) {
            delete temp[key];
        }
    }

    if (Object.keys(temp).length == 0) {
        return;
    }

    let setString: string = "";
    for (let key of Object.keys(temp)) {
        let value;
        if (typeof temp[key] == "number" || typeof temp[key] == "boolean") {
            value = temp[key];
        } else if (typeof temp[key] == "string") {
            if (temp[key].includes("{")) {
                value = `'${temp[key].replaceAll("'", "''")}'`;
            } else {
                value = `'${temp[key].replaceAll("'", "''").replaceAll("\"", "''")}'`;
            }
        } else if (typeof temp[key] == "object") {
            value = `'{"${temp[key].join("\", \"").replaceAll("'", "''")}"}'`;
        }

        setString += `${key}=${value},`;
    }

    if (setString == "") {
        return;
    }

    setString = setString.slice(0, -1);

    let whereString = "";
    for (let arr of where) {
        if (typeof arr[1] == "number" || typeof arr[1] == "boolean") {
            whereString += `${arr[0]}=${arr[1]} AND `;
        } else if (typeof arr[1] == "string") {
            whereString += `${arr[0]}='${arr[1].replaceAll("'", "''")}' AND `;
        } else if (typeof arr[1] == "object" && arr[1] != null) {
            whereString += `${arr[0]}='{"${arr[1].join("\", \"").replaceAll("'", "''")}"}' AND `;
        }
    }

    whereString = whereString.slice(0, -5);

    const conn = await client.connect();

    await conn.query(`UPDATE ${table}
                          SET ${setString}
                          WHERE ${whereString}`)
        .then(async () => {
        })
        .catch(async (e: any) => {
            num++;
            if (num == 2) {
                throw new ThrowError(`UPDATE ${table} SET ${setString} WHERE ${whereString}`);
            }
            await update(client, temp, table, where, num);
        });

    conn.release();
}

export async function has (client: Pool, query: string, num: number = 0): Promise<boolean | null> {
    const conn = await client.connect();

    let passed: any = false;

    await conn
        .query(query)
        .then(async (res: { rows: any[]; }) =>  {
            passed = res.rows.length > 0;
        })
        .catch(async(e: { stack: any; }) => {
            num++;
            if (num == 2) {
                throw new ThrowError(query);
            }
            await has(client, query, num);
        });

    conn.release();

    return passed;
}

export async function getValue (client: Pool, query: string, num: number = 0): Promise<any> {
    const conn = await client.connect();

    let result: any[] | null = null;
    
    await conn
        .query(query)
        .then(async (res: { rows: any[]; }) =>  {
            result = res.rows;
        })
        .catch(async (e: { stack: any; }) => {
            num++;
            if (num == 2) {
                throw new ThrowError(query);
            }
            await getValue(client, query, num);
        });

    await conn.release();

    return result;
}