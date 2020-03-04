# PostgreSQL Database Reducer

PostgreSQL Database Reducer is a tool for reducing large PostgreSQL databases to smaller versions for test environments. It works by first turning off the FK checks, then deleting rows that do not match the specified criteria and rows which are orphaned after the aforementioned deletion and then re-enabling the FK checks.

## Installation

The tool is a single self-contained executable file. No need for an installation. If you are on a Unix-like or otherwise POSIX-compliant system, including Linux-based systems and all macOS versions, you must make the downloaded file executable:

```bash
chmod +x postgresql-database-reducer
```

## Usage

```bash
./postgresql-database-reducer -h=127.0.0.1 -p=5432 -d=myLiveDBCopy -u=postgres -p=secret -r=1000
```
This keeps the first 1000 rows in each table. After that it scans for orphaned rows (rows which refer to deleted rows in other tables) and deletes them as well.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.


## License
[MIT](https://choosealicense.com/licenses/mit/)