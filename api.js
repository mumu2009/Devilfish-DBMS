// api.js
const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = 3000;

app.use(bodyParser.json());

// 引入HypercubeDB类
const { HypercubeDB } = require('./main.js');
const db = new HypercubeDB();

// 定义API路由
app.post('/createDatabase', (req, res) => {
    const { name } = req.body;
    db.createDatabase(name);
    res.send('Database created');
});

app.post('/useDatabase', (req, res) => {
    const { name } = req.body;
    db.useDatabase(name);
    res.send('Database selected');
});

app.post('/createTable', (req, res) => {
    const { tableName, schema } = req.body;
    db.createTable(tableName, schema);
    res.send('Table created');
});

app.post('/addDimension', (req, res) => {
    const { tableName, dimension } = req.body;
    db.addDimension(tableName, dimension);
    res.send(`Dimension ${dimension} added`);
});

app.post('/removeDimension', (req, res) => {
    const { tableName, dimension } = req.body;
    db.removeDimension(tableName, dimension);
    res.send(`Dimension ${dimension} removed`);
});

app.post('/insertData', (req, res) => {
    const { tableName, data } = req.body;
    db.insertData(tableName, data);
    res.send('Data inserted');
});

app.post('/selectData', (req, res) => {
    const { tableName, query } = req.body;
    db.selectData(tableName, query);
    res.send('Selected data');
});

app.post('/getFuzzyFunction', (req, res) => {
    const { tableName, dim1, dim2 } = req.body;
    const fuzzyFunction = db.getFuzzyFunction(tableName, dim1, dim2);
    res.json(fuzzyFunction);
});

app.post('/taylorExpand', (req, res) => {
    const { tableName, dim1, dim2, x0, y0, order } = req.body;
    const terms = db.taylorExpand(tableName, dim1, dim2, x0, y0, order);
    res.json(terms);
});

app.listen(port, () => {
    console.log(`API server running at http://localhost:${port}`);
});