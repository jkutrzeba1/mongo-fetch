
const { MongoClient } = require("mongodb");
const ObjectID = require('mongodb').ObjectID;

const fs = require("fs");
const path = require("path");

let sellers = {};

let productsCollection;

for(let filename of fs.readdirSync(path.resolve(""))){
	
	let regres = filename.match(/^ua@(.+)@(.+)_OUT/);
	
	if(!regres || !regres[1]) continue;
	
	let producent = regres[1];
	let seller = regres[2];
	
	let productsIn = fs.readFileSync( path.resolve("", filename) ,"utf-8").split("\n");

	if(productsIn[productsIn.length-1] == "") productsIn.splice(productsIn.length-1, 1);
	
	for(let srcLine of productsIn){
		
		let [code, title, price, href] = srcLine.split("\t");
		
		price = parseFloat(price.replace(/,/, "."));
		
		if(!sellers[seller]) sellers[seller] = {};
		if(!sellers[seller][producent]) sellers[seller][producent] = []
		
		sellers[seller][producent].push({
			code, title, price, seller, producent, href
		})
		
	}

}


const url = "mongodb://u:p@host.com:27017/db";

const mongoClient = new MongoClient(url);

const dbReady = mongoClient.connect();

let today = new Date();

function fetch( seller, producent, x){
	
	cursor = getCursor(seller, producent, x++);
	
	return cursor.toArray()
	.then((docs)=>{
		
		if(docs.length == 0) return false;

		console.log("fetched " + docs.length + " documents");
		
		let ready = Promise.resolve();
		
		for(let doc of docs){
			
			for(let product of sellers[seller][producent]){
				
				ready = ready.then(()=>{
					
					if(product.code == doc.code){
						product.alreadyInDb = true
					}
				
					if(product.code == doc.code && product.price != doc.price){
						
						console.log(product.code);
						console.log(doc.price + " --> " + product.price)
						
						return productsCollection.updateOne( {_id: new ObjectID(doc._id) }, {$set: {price: product.price, updatedAt: today}})
					}
					
				})
				
			}
			
		}
		
		return ready.then(()=>{
			
			return true;
		})

	})
	.then((toBeContinued)=>{
		
		cursor.close()
		
		if(toBeContinued) return fetch( seller, producent, x);
		
		//recurrency exit
		//documents from all chunksare fetched, all documents that are marked as alreadyInDb are filtered and new documents are inserted
		
		let newProducts = sellers[seller][producent]
		.filter((product)=>{
			if(product.alreadyInDb) return false;
			
			return true;
		})
		.map((product)=>{
			return {
				...product,
				createdAt: today
			}
		});
		
		if(newProducts.length>0){
			
			return productsCollection.insertMany(newProducts)
			.then((res)=>{
				console.log(newProducts);
				console.log("nowe rekordy: " + res.insertedCount);
			});
		}
		
	});
	
}

function getCursor(seller, producent, x){
	
	return productsCollection.aggregate([
		{
			$match: {
				seller: seller,
				producent: producent
			}
		},
		{
			$project: {
				_id: 1,
				code: 1,
				price: 1
			}
		},
		{
			$skip: x*100
		},
		{
			$limit: 100
		}
	]);
					
	
}

function updateModifiedAndInsertNew(){ 
	
	const pricetop = mongoClient.db();
	
	productsCollection = pricetop.collection("products");
	
	let ready = Promise.resolve();
	
	for(let seller in sellers){
		
		for(let producent in sellers[seller]){
			
			ready = ready.then(()=>{
				
				return fetch( seller, producent, 0);
			})

		}
		
	}
	
}

function deleteDuplicates(){
	
	const pricetop = mongoClient.db();
	
	productsCollection = pricetop.collection("products");
	
	const aggregation = 
	[
	  {
		'$group': {
		  '_id': {
			'seller': '$seller', 
			'producent': '$producent', 
			'code': '$code'
		  }, 
			'ids':	{ '$addToSet': '$_id' }
		}
		}, {
		'$project': {
		  'ids': true, 
		  'duplicate': {
			'$gt': [
			  {
				'$size': '$ids'
			  }, 1
			]
		  }, 
		  'size': {
			'$size': '$ids'
		  }
		}
	  }, {
		'$match': {
		  'duplicate': true
		}
	  }, {
		'$project': {
		  'ids': {
			'$slice': [
			  '$ids', 1, {
				'$add': [
				  {
					'$size': '$ids'
				  }, -1
				]
			  }
			]
		  }
		}
	  }
	];
	
	
	const cursor = productsCollection.aggregate(aggregation);
	
	let idsDesiredToDeletion = [];
	
	cursor.toArray()
	.then((docs)=>{
		
		for(let doc of docs){
			idsDesiredToDeletion = [...idsDesiredToDeletion, ...doc.ids];
		}
		
	})
	.then(()=>{
		
		return productsCollection.deleteMany({ _id: { $in: idsDesiredToDeletion } })
		.then((res)=>{
			console.log(res);
		})
	})
	
}

dbReady.then((client)=>{
	
	updateModifiedAndInsertNew()
	
});

dbReady.catch((err)=>{
	
	throw err;
})
