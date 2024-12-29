//this is a design for the DBMS and detabase
//use GPL3.0 lisense

const fs = require('fs');
const readline = require('readline');
const regexp = require('regexp');
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});
const buffer =require('buffer');
const events=require('events');
const eventEmitter=new events.EventEmitter();
const jsonParser = require('json-parser');
const path=require('path');
var pointer
console.log("welcome to the Devilfish DBMS system");


const zlib = require('zlib');
const util = require('util');

// 添加日志函数
function log(level, message) {
    const timestamp = new Date().toISOString();
    const logMessage = `${timestamp} [${level}] ${message}`;
    console.log(logMessage);
    fs.appendFileSync('dbms.log', logMessage + '\n');
}

// 错误处理函数
function handleError(error, operation) {
    log('ERROR', `Error during ${operation}: ${error.message}`);
    console.error(`An error occurred during ${operation}. Please check the logs for more details.`);
}

class HypercubeDB {
    constructor() {
        this.data = {};
        this.indexes = {};
        this.locks = {};
        this.loadData();
    }

    loadData() {
        try {
        const dbFiles = fs.readdirSync(__dirname);
        dbFiles.forEach(file => {
            if (file.endsWith('.db')) {
                const dbName = file.slice(0, -3);
                const rawData = fs.readFileSync(path.join(__dirname, file));
                const decompressedData = zlib.gunzipSync(rawData);
                this.data[dbName] = JSON.parse(decompressedData.toString());
            }
        });
        log('INFO', 'Data loaded successfully');
    } catch (error) {
        handleError(error, 'loading data');
     }
    }

    saveData() {
        try {
        
        Object.keys(this.data).forEach(dbName => {
            const rawData = JSON.stringify(this.data[dbName], null, 2);
            const compressedData = zlib.gzipSync(rawData);
            fs.writeFileSync(path.join(__dirname, `${dbName}.db`), compressedData);
        });
        log('INFO', 'Data saved successfully');
    } catch (error) {
        handleError(error, 'saving data');
    }
    }

    acquireLock(tableName) {
        if (!this.locks[tableName]) {
            this.locks[tableName] = true;
            log('DEBUG', `Lock acquired for table ${tableName}`);
            return true;
        }
        log('DEBUG', `Failed to acquire lock for table ${tableName}`);
        return false;
    }
    
    releaseLock(tableName) {
        delete this.locks[tableName];
        log('DEBUG', `Lock released for table ${tableName}`);
    }

    createDatabase(name) {
        try{
        if (this.data[name]) {
            console.log("Database already exists");
        } else {
            this.data[name] = {};
            this.saveData();
            console.log("Database created");
        }
        log('INFO', 'Database created successfully');
        } catch (error) {
            handleError(error, 'creating database');
        }
    }

    useDatabase(name) {
        try{
        if (this.data[name]) {
            this.currentDatabase = name;
            console.log("Database selected");
        } else {
            console.log("Database does not exist");
        }
        log('INFO', 'Database selected successfully');
        } catch (error) {
            handleError(error, 'using database');
        }
    }

    createTable(tableName, schema) {
        try{
        if (!this.currentDatabase) {
            console.log("No database selected");
            return;
        }
        if (this.data[this.currentDatabase][tableName]) {
            console.log("Table already exists");
        } else {
            if (this.acquireLock(tableName)) {
                this.data[this.currentDatabase][tableName] = {};
                this.indexes[tableName] = {};
                schema.forEach(dim => {
                    this.indexes[tableName][dim] = {};
                });
                this.saveData();
                this.releaseLock(tableName);
                console.log("Table created");
            } else {
                console.log("Table is locked");
            }
        }
        log('INFO', 'Table created successfully');
        } catch (error) {
            handleError(error, 'creating table');
        }
    }

    addDimension(tableName, dimension) {
        try{
        if (!this.currentDatabase) {
            console.log("No database selected");
            return;
        }
        if (!this.data[this.currentDatabase][tableName]) {
            console.log("Table does not exist");
            return;
        }
        if (this.acquireLock(tableName)) {
            if (!this.indexes[tableName][dimension]) {
                this.indexes[tableName][dimension] = {};
                Object.keys(this.data[this.currentDatabase][tableName]).forEach(key => {
                    const data = this.data[this.currentDatabase][tableName][key];
                    if (data[dimension] !== undefined) {
                        if (!this.indexes[tableName][dimension][data[dimension]]) {
                            this.indexes[tableName][dimension][data[dimension]] = [];
                        }
                        this.indexes[tableName][dimension][data[dimension]].push(data);
                    }
                });
            }
            this.saveData();
            this.releaseLock(tableName);
            console.log(`Dimension ${dimension} added`);
        } else {
            console.log("Table is locked");
        }
        log('INFO', 'Dimension added successfully');
        } catch (error) {
            handleError(error, 'adding dimension');
        }
    }

    removeDimension(tableName, dimension) {
        try{
        if (!this.currentDatabase) {
            console.log("No database selected");
            return;
        }
        if (!this.data[this.currentDatabase][tableName]) {
            console.log("Table does not exist");
            return;
        }
        if (this.acquireLock(tableName)) {
            if (this.indexes[tableName][dimension]) {
                delete this.indexes[tableName][dimension];
                Object.keys(this.data[this.currentDatabase][tableName]).forEach(key => {
                    const data = this.data[this.currentDatabase][tableName][key];
                    delete data[dimension];
                });
            }
            this.saveData();
            this.releaseLock(tableName);
            console.log(`Dimension ${dimension} removed`);
        } else {
            console.log("Table is locked");
        }
        log('INFO', 'Dimension removed successfully');
        } catch (error) {
            handleError(error, 'Removing dimension');
    
        }
    }

    // ... previous code ...

    insertData(tableName, data) {
        try {
            if (!this.currentDatabase) {
                console.log("No database selected");
                return;
            }
            if (!this.data[this.currentDatabase][tableName]) {
                console.log("Table does not exist");
                return;
            }
            if (this.acquireLock(tableName)) {
                // Check if data is a valid object
                if (typeof data !== 'object' || data === null) {
                    console.log("Invalid data format");
                    this.releaseLock(tableName);
                    return;
                }

                // Convert data to a valid JSON object if it's a string
                if (typeof data === 'string') {
                    try {
                        data = JSON.parse(data);
                    } catch (error) {
                        console.log("Invalid JSON format:", error.message);
                        this.releaseLock(tableName);
                        return;
                    }
                }

                // Check if data has valid property names
                const validData = {};
                Object.keys(data).forEach(key => {
                    if (typeof key === 'string' && key.trim() !== '') {
                        validData[key.trim()] = data[key];
                    } else {
                        console.log("Invalid property name in data:", key);
                        this.releaseLock(tableName);
                        return;
                    }
                });

                data = validData;

                const key = Object.keys(data).join('-');
                this.data[this.currentDatabase][tableName][key] = data;

                // Index the data based on dimensions
                Object.keys(data).forEach(dim => {
                    if (this.indexes[tableName][dim] !== undefined) {
                        if (!this.indexes[tableName][dim][data[dim]]) {
                            this.indexes[tableName][dim][data[dim]] = [];
                        }
                        this.indexes[tableName][dim][data[dim]].push(data);
                    }
                });

                this.saveData();
                this.releaseLock(tableName);
                console.log("Data inserted");
            } else {
                console.log("Table is locked");
            }
            log('INFO', 'Data inserted successfully');
        } catch (error) {
            handleError(error, 'inserting data');
        }
    }
    dropTable(tableName) {
        try {
            if (!this.currentDatabase) {
                console.log("No database selected");
                return;
            }
            if (this.acquireLock(tableName)) {
                if (this.data[this.currentDatabase][tableName]) {
                    delete this.data[this.currentDatabase][tableName];
                    delete this.indexes[tableName];
                    this.saveData();
                    this.releaseLock(tableName);
                    console.log("Table dropped");
                } else {
                    console.log("Table does not exist");
                }
                log('INFO', 'Table dropped successfully');
            } else {
                console.log("Table is locked");
            }
        } catch (error) {
            handleError(error, 'dropping table');
        }
    }
    deleteData(tableName, key) {
        try {
            if (!this.currentDatabase) {
                console.log("No database selected");
                return;
            }
            if (!this.data[this.currentDatabase][tableName]) {
                console.log("Table does not exist");
                return;
            }
            if (this.acquireLock(tableName)) {
                if (this.data[this.currentDatabase][tableName][key]) {
                    // Delete data
                    delete this.data[this.currentDatabase][tableName][key];
    
                    // Re-index the data based on dimensions
                    Object.keys(this.indexes[tableName]).forEach(dim => {
                        Object.keys(this.indexes[tableName][dim]).forEach(value => {
                            this.indexes[tableName][dim][value] = this.indexes[tableName][dim][value].filter(data => data[key] !== undefined);
                        });
                    });
    
                    this.saveData();
                    this.releaseLock(tableName);
                    console.log("Data deleted");
                } else {
                    console.log("Data not found");
                }
                log('INFO', 'Data deleted successfully');
            } else {
                console.log("Table is locked");
            }
        } catch (error) {
            handleError(error, 'deleting data');
        }
    }
    updateData(tableName, key, newData) {
        try {
            if (!this.currentDatabase) {
                console.log("No database selected");
                return;
            }
            if (!this.data[this.currentDatabase][tableName]) {
                console.log("Table does not exist");
                return;
            }
            if (this.acquireLock(tableName)) {
                if (this.data[this.currentDatabase][tableName][key]) {
                    // Update data
                    Object.assign(this.data[this.currentDatabase][tableName][key], newData);
    
                    // Re-index the data based on dimensions
                    Object.keys(newData).forEach(dim => {
                        if (this.indexes[tableName][dim] !== undefined) {
                            if (!this.indexes[tableName][dim][newData[dim]]) {
                                this.indexes[tableName][dim][newData[dim]] = [];
                            }
                            this.indexes[tableName][dim][newData[dim]].push(newData);
                        }
                    });
    
                    this.saveData();
                    this.releaseLock(tableName);
                    console.log("Data updated");
                } else {
                    console.log("Data not found");
                }
                log('INFO', 'Data updated successfully');
            } else {
                console.log("Table is locked");
            }
        } catch (error) {
            handleError(error, 'updating data');
        }
    }
    selectData(tableName, query) {
        try {
            if (!this.currentDatabase) {
                console.log("No database selected");
                return;
            }
            if (!this.data[this.currentDatabase][tableName]) {
                console.log("Table does not exist");
                return;
            }
            if (this.acquireLock(tableName)) {
                try {
                    let results = Object.values(this.data[this.currentDatabase][tableName]);
                    Object.keys(query).forEach(dim => {
                        if (this.indexes[tableName][dim]) {
                            if (typeof query[dim] === 'string') {
                                if (query[dim].includes('*')) {
                                    // Support for wildcard queries
                                    const wildcardPattern = query[dim].replace(/\*/g, '.*');
                                    const regex = new RegExp(wildcardPattern);
                                    results = results.filter(data => regex.test(data[dim]));
                                } else {
                                    results = results.filter(data => data[dim] === query[dim]);
                                }
                            } else if (typeof query[dim] === 'object') {
                                // Support for coordinate range queries
                                const start = query[dim].start;
                                const end = query[dim].end;
                                const step = query[dim].step || 1; // 修正了这里的默认值获取方式
                                results = results.filter(data => {
                                    const value = parseFloat(data[dim]);
                                    return value >= start && value <= end && (value - start) % step === 0;
                                });
                            }
                        }
                    });
                    console.log("Selected data:", results);
                } finally {
                    this.releaseLock(tableName);
                }
            } else {
                console.log("Table is locked");
            }
            log('INFO', 'Data selected successfully');
        } catch (error) {
            handleError(error, 'selecting data');
        }
    }

    getFuzzyFunction(tableName, dim1, dim2) {
        if (!this.currentDatabase) {
            console.log("No database selected");
            return;
        }
        if (!this.data[this.currentDatabase][tableName]) {
            console.log("Table does not exist");
            return;
        }
        const dataPoints = Object.values(this.data[this.currentDatabase][tableName]);
        const filteredData = dataPoints.filter(data => data[dim1] !== undefined && data[dim2] !== undefined);
        return (x, y) => {
            // Simple linear interpolation
            let sum = 0;
            let count = 0;
            filteredData.forEach(data => {
                sum += data[dim1] * x + data[dim2] * y;
                count++;
            });
            return count > 0 ? sum / count : 0;
        };
    }

    taylorExpand(tableName, dim1, dim2, x0, y0, order) {
        const fuzzyFunction = this.getFuzzyFunction(tableName, dim1, dim2);
        const terms = [];
        for (let i = 0; i <= order; i++) {
            for (let j = 0; j <= order - i; j++) {
                const term = this.taylorTerm(fuzzyFunction, dim1, dim2, x0, y0, i, j);
                terms.push(term);
            }
        }
        return terms;
    }

    taylorTerm(fuzzyFunction, dim1, dim2, x0, y0, i, j) {
        const h = 1e-5; // small step size
        let derivative = 0;
        for (let k = 0; k <= i; k++) {
            for (let l = 0; l <= j; l++) {
                const sign = Math.pow(-1, k + l);
                const coeff = Math.pow(h, k + l) / (factorial(k) * factorial(l));
                const x = x0 + (i - k) * h;
                const y = y0 + (j - l) * h;
                derivative += sign * coeff * fuzzyFunction(x, y);
            }
        }
        return derivative * Math.pow(x0, i) * Math.pow(y0, j) / factorial(i) / factorial(j);
    }
    importFromCSV(tableName, filePath) {
        if (!this.currentDatabase) {
            console.log("No database selected");
            return;
        }
        if (!this.data[this.currentDatabase][tableName]) {
            console.log("Table does not exist");
            return;
        }
        if (this.acquireLock(tableName)) {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (row) => {
                    this.insertData(tableName, row);
                })
                .on('end', () => {
                    console.log('CSV file successfully imported');
                    this.releaseLock(tableName);
                });
        } else {
            console.log("Table is locked");
        }
    }

}



function factorial(n) {
    if (n === 0 || n === 1) return 1;
    return n * factorial(n - 1);
}
const db = new HypercubeDB();
function displayPrompt(){
    process.stdout.write('>');
}
rl.on('line', (answer) => {
    const reg1 = new RegExp("^\\s*create\\s+database\\s+([a-zA-Z0-9_]+)\\s*$");
    const reg2 = new RegExp("^\\s*use\\s+([a-zA-Z0-9_]+)\\s*$");
    const reg3 = new RegExp("^\\s*create\\s+table\\s+([a-zA-Z0-9_]+)\\s*\\(([a-zA-Z0-9_]+(,[a-zA-Z0-9_]+)*)\\)\\s*$");
    const reg4 = new RegExp("^\\s*insert\\s+into\\s+([a-zA-Z0-9_]+)\\s+values\\s*\\{([^}]+)\\}\\s*$");
    const reg5 = new RegExp("^\\s*select\\s+\\*\\s+from\\s+([a-zA-Z0-9_]+)\\s+where\\s+([^=]+)=([^=]+)\\s*$");
    const reg6 = new RegExp("^\\s*exit\\s*$");
    const reg7 = new RegExp("^\\s*add\\s+dimension\\s+([a-zA-Z0-9_]+)\\s+to\\s+([a-zA-Z0-9_]+)\\s*$");
    const reg8 = new RegExp("^\\s*remove\\s+dimension\\s+([a-zA-Z0-9_]+)\\s+from\\s+([a-zA-Z0-9_]+)\\s*$");
    const reg9 = new RegExp("^\\s*get\\s+fuzzy\\s+function\\s+([a-zA-Z0-9_]+)\\s+on\\s+([a-zA-Z0-9_]+)\\s+and\\s+([a-zA-Z0-9_]+)\\s*$");
    const reg10 = new RegExp("^\\s*taylor\\s+expand\\s+([a-zA-Z0-9_]+)\\s+on\\s+([a-zA-Z0-9_]+)\\s+and\\s+([a-zA-Z0-9_]+)\\s+at\\s+\\(([^,]+),([^)]+)\\)\\s+order\\s+(\\d+)\\s*$");
    const reg11 = new RegExp("^\\s*import\\s+csv\\s+([a-zA-Z0-9_]+)\\s+from\\s+([^\\s]+)\\s*$");
    const reg12 = new RegExp("^\\s*update\\s+([a-zA-Z0-9_]+)\\s+set\\s+\\{([^}]+)\\}\\s+where\\s+([^=]+)=([^=]+)\\s*$");
    const reg13 = new RegExp("^\\s*delete\\s+from\\s+([a-zA-Z0-9_]+)\\s+where\\s+([^=]+)=([^=]+)\\s*$");
    const reg14 = new RegExp("^\\s*drop\\s+table\\s+([a-zA-Z0-9_]+)\\s*$");
    const regSelectData = new RegExp("^\\s*select\\s+\\*\\s+from\\s+([a-zA-Z0-9_]+)\\s+(where\\s+([^=]+)\\s*(=|>|<|>=|<=|<>)\\s*([^=]+))?\\s*$");
    if (reg1.test(answer)) {
        const dbName = reg1.exec(answer)[1];
        db.createDatabase(dbName);
    } else if (reg2.test(answer)) {
        const dbName = reg2.exec(answer)[1];
        db.useDatabase(dbName);
    } else if (reg3.test(answer)) {
        const match = reg3.exec(answer);
        if (match && match[2]) {
            const tableName = match[1];
            const schemaStr = match[2];
            const schema = schemaStr.split(',');
            db.createTable(tableName, schema);
        } else {
            console.log("Invalid command format for create table");
        }
    } else if (reg4.test(answer)) {
        const tableName = reg4.exec(answer)[1];
        const dataStr = reg4.exec(answer)[2];
    
        // 将键名转换为字符串形式
        const formattedDataStr = dataStr.replace(/(\w+):/g, '"$1":');
    
        try {
            const data = JSON.parse(`{${formattedDataStr}}`);
            db.insertData(tableName, data);
        } catch (error) {
            console.log("Invalid JSON format:", error.message);
        }
    } else if (reg5.test(answer)) {
        const dim = reg5.exec(answer)[1];
        const tableName = reg5.exec(answer)[2];
        const key = reg5.exec(answer)[3].trim();
        const value = reg5.exec(answer)[4].trim();
        db.selectData(tableName, { [key]: value });
    } else if (reg6.test(answer)) {
        rl.close();
    } else if (reg7.test(answer)) {
        const dimension = reg7.exec(answer)[1];
        const tableName = reg7.exec(answer)[2];
        db.addDimension(tableName, dimension);
    } else if (reg8.test(answer)) {
        const dimension = reg8.exec(answer)[1];
        const tableName = reg8.exec(answer)[2];
        db.removeDimension(tableName, dimension);
    } else if (reg9.test(answer)) {
        const tableName = reg9.exec(answer)[1];
        const dim1 = reg9.exec(answer)[2];
        const dim2 = reg9.exec(answer)[3];
        const fuzzyFunction = db.getFuzzyFunction(tableName, dim1, dim2);
        console.log(`Fuzzy function on ${dim1} and ${dim2}:`, fuzzyFunction);
    } else if (reg10.test(answer)) {
        const tableName = reg10.exec(answer)[1];
        const dim1 = reg10.exec(answer)[2];
        const dim2 = reg10.exec(answer)[3];
        const x0 = parseFloat(reg10.exec(answer)[4]);
        const y0 = parseFloat(reg10.exec(answer)[5]);
        const order = parseInt(reg10.exec(answer)[6]);
        const terms = db.taylorExpand(tableName, dim1, dim2, x0, y0, order);
        console.log(`Taylor expansion of fuzzy function on ${dim1} and ${dim2} at (${x0}, ${y0}) order ${order}:`, terms);
    } else if (reg11.test(answer)) {
        const tableName = reg11.exec(answer)[1];
        const filePath = reg11.exec(answer)[2];
        db.importFromCSV(tableName, filePath);
    } else if (reg12.test(answer)) {
        const match = reg12.exec(answer);
        const tableName = match[1];
        const key = match[3];
        const newDataStr = match[2];
        const formattedDataStr = newDataStr.replace(/(\w+):/g, '"$1":');
        try {
            const newData = JSON.parse(`{${formattedDataStr}}`);
            db.updateData(tableName, key, newData);
        } catch (error) {
            console.log("Invalid JSON format:", error.message);
        }
    } else if (reg13.test(answer)) {
        const tableName = reg13.exec(answer)[1];
        const key = reg13.exec(answer)[3];
        db.deleteData(tableName, key);
    } else if (reg14.test(answer)) {
        const tableName = reg14.exec(answer)[1];
        db.dropTable(tableName);
    }else if (regSelectData.test(answer)) {
        const match = regSelectData.exec(answer);
        const tableName = match[1];
        const query = {};

        if (match[2]) {
            const dim = match[3].trim();
            const operator = match[4];
            const value = match[5].trim();

            switch (operator) {
                case '=':
                    query[dim] = value;
                    break;
                case '>':
                    query[dim] = { start: parseFloat(value) + 1, end: Infinity, step: 1 };
                    break;
                case '<':
                    query[dim] = { start: -Infinity, end: parseFloat(value) - 1, step: 1 };
                    break;
                case '>=':
                    query[dim] = { start: parseFloat(value), end: Infinity, step: 1 };
                    break;
                case '<=':
                    query[dim] = { start: -Infinity, end: parseFloat(value), step: 1 };
                    break;
                case '<>':
                    query[dim] = { start: -Infinity, end: parseFloat(value) - 1, step: 1 };
                    query[dim] = { start: parseFloat(value) + 1, end: Infinity, step: 1 };
                    break;
                default:
                    console.log("Invalid operator");
                    return;
            }
        }

        db.selectData(tableName, query);
    }else{
        console.log("Invalid command,please use available commands or write 'exit'to escape");
    }
    displayPrompt();
});

displayPrompt();

rl.on('close', () => {
    console.log('thank you for using Devilfish DBMS system ,see you next time!');
    process.exit(0);
});