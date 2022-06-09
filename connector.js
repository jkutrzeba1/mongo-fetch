
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


const url = "mongodb://u:p@host.com/dbname";

const mongoClient = new MongoClient(url);

const dbReady = mongoClient.connect();

function fetch(cursor, seller, producent, x){
	
	return cursor.hasNext()
	.then((hasNext)=>{
		
		if(!hasNext) return false;
		
		cursor.rewind();

		return cursor.skip(x++*100).limit(100)
		.toArray()
		.then((docs)=>{
			
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
							
							return productsCollection.updateOne( {_id: new ObjectID(doc._id) }, {$set: {price: product.price, updatedAt: new Date()}})
						}
						
					})
					
				}
				
			}
			
			return ready.then(()=>{
				
				return true;
			})
			
		})

	})
	.then((toBeContinued)=>{
		
		if(toBeContinued) return fetch(cursor, seller, producent, x);
		
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
				createdAt: new Date()
			}
		});
		
		if(newProducts.length>0){
			return productsCollection.insertMany(newProducts)
			.then((res)=>{
				console.log(res);
			})
		}
		
	});
	
}

function updateModifiedAndInsertNew(){ 
	
	const pricetop = mongoClient.db();
	
	productsCollection = pricetop.collection("products");
	
	let ready = Promise.resolve();
	
	for(let seller in sellers){
		
		for(let producent in sellers[seller]){
			
			ready = ready.then(()=>{
				
				const cursor = productsCollection.aggregate([
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
					}
				]);
				

								
				return fetch(cursor, seller, producent, 0);
				
			})

		}
		
	}
	
}

dbReady.then((client)=>{
	
	updateModifiedAndInsertNew()
	
});

dbReady.catch((err)=>{
	
	throw err;
})
