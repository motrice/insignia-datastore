extern crate rusoto_core;
extern crate rusoto_dynamodb;
extern crate uuid;
extern crate serde;
extern crate serde_dynamodb;
#[macro_use] extern crate serde_derive;
extern crate tokio;
extern crate chrono;
use chrono::{DateTime, Utc};

use std::collections::HashMap;
use rusoto_core::Region;
use rusoto_dynamodb::{
    AttributeDefinition, 
    AttributeValue,
    CreateTableInput, 
    DeleteTableInput, 
    DynamoDb, 
    DynamoDbClient, 
    GetItemInput,
    GlobalSecondaryIndex,
    KeySchemaElement, 
    ListTablesInput, 
    Projection,
    ProvisionedThroughput,
    PutItemInput,
    QueryInput,
    ScanInput
};

use uuid::Uuid;

pub mod domain;

use insignia_datastore::domain::*;

use insignia_datastore::*;

// pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
// pub type Result<T> = std::result::Result<T, GenericError>;


#[tokio::main]
pub async fn main() -> Result<()> {


        println!("Hello, world!");

    let region = Region::Custom {
        name: "eu-north-1".to_owned(),
        endpoint: "http://localhost:8000".to_owned(),
    };

    let client = insignia_datastore::GraphDb::new();

    match client.client.delete_table(DeleteTableInput{table_name: String::from("bm-test-table")}).sync() {
        Ok(_) => println!("Deleted table"),
        Err(_) => println!("Delete failed")
    }
/*
user-nnnn   user-nnnn       givenName, surname
user-nnnn   personalNumber  personalNumber
            email
            phone
            adress
            name            name
org-nnnn    org-nnnn        "org data"
org-nnnn    user-nnnn       "org user role"

doc-nnnn    doc-nnnn        "title: String, , created, modified"
doc-nnnn    s3-ref
doc-nnnn    checksum          
doc-nnnn    user-nnnn       "user role"
doc-nnnn    org-nnnn        "org role"
doc-nnnn    signreq-nnnn    "sign req"
doc-nnnn    signature-nnnn  "signature data"
doc-nnnn    link            "permissons"

{
  "IndexName" : String,
  "KeySchema" : [ KeySchema, ... ],
  "Projection" : Projection,
  "ProvisionedThroughput" : ProvisionedThroughput
}

*/
    match client.client.create_table(CreateTableInput{
        table_name: String::from("insignia-docs"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "vertex_a".into(),
                key_type: "HASH".into(),
            }, 
            KeySchemaElement {
                attribute_name: "edge".into(),
                key_type: "RANGE".into(),
            }
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "vertex_a".into(),
                attribute_type: "S".into(),
            },
            AttributeDefinition {
                attribute_name: "vertex_b".into(),
                attribute_type: "S".into(),
            },
            AttributeDefinition {
                 attribute_name: "edge".into(),
                 attribute_type: "S".into(),
            }
        ],
        global_secondary_indexes: Some(vec![
            GlobalSecondaryIndex{
                index_name: "index-vertex_b_edges".into(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "vertex_b".into(),
                        key_type: "HASH".into(),
                    }, 
                    KeySchemaElement {
                        attribute_name: "edge".into(),
                        key_type: "RANGE".into(),
                    }
                ],
                projection: Projection {
                    non_key_attributes: None,
                    projection_type: Some("ALL".into())

                },
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 1,
                    write_capacity_units: 1,
                })
            }
        ]),
        provisioned_throughput: Some(ProvisionedThroughput {
            read_capacity_units: 1,
            write_capacity_units: 1,
        }),
        ..CreateTableInput::default()
    }).sync() {
        Ok(out) => println!("Sucess! {:?}", out),
        Err(err) => println!("Error {:?}", err)
    }
    
    let mut user1 = client.new_user("191212121212", "Tolvan Tolvansson", "Tolvan", "Tolvansson", Some("tolvan.tolvansson@motrice.se"), Some("+46733414983"), None)?;
    
    match client.upload_document_url(&user1.parse()?) {
        Ok(todo_doc_id) => {
            match client.upload_document_completed(&todo_doc_id, "foobucket", "barkey", "1234checksum") {
                Ok(_) => println!("upload doc completed"),
                Err(err) => println!("upload doc err {}", err)
            }
        },
        Err(err) => println!("Error {}", err)
    }

    let scan_edges : Vec<Edge> = match client.client.scan(ScanInput{
        table_name: String::from("insignia-docs"),
        ..ScanInput::default()
    }).sync(){
            Ok(res) => {
                res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
            },
            Err(err) =>  {
                println!("Error query{:?}", err);
                vec![]
            }
    };

    for item in &scan_edges {
        println!("Scan Edge {} {} {} {:?}", item.vertex_b, item.edge, item.vertex_a, item.data);
    }

    for item in &scan_edges {
        let edges = client.get_vertex_with_edges(&item.vertex_a).unwrap();
        println!("Vertex: {}", &item.vertex_a);
        for edge in &edges {
            //println!("    Edge {} {} {} {:?}", edge.vertex_b, edge.edge, edge.vertex_a, edge.data);
            println!("    Edge {} {:?}", edge.edge, edge.data);
            
        }
    }

    let users = client.get_users_by_personal_number("191212121212");
    for item in &users {
        println!("Tolvan user: {}", item);
    }

    match client.session_new() {
        Ok(session) => {
            println!("new session:            {}", &session);
            match client.session_get(&session.session_id) {
                Some(loaded_sess) =>  {
                    println!("Loaded the new session: {}", &loaded_sess);
                },
                None => println!("Could not load new session")
            }
            match client.session_auth(&session.session_id, &users[0].user_id, "logged in from ip 123.456.7.8") {
                Ok(sess_auth) =>  {
                    println!("auth session:            {}", &session);
                    match client.session_get(&sess_auth.session_id) {
                        Some(loaded_sess) =>  {
                            println!("Loaded the auth session: {}", &loaded_sess);
                        },
                        None => println!("Could not load new session")
                    }

                    match client.session_logout(&sess_auth.session_id) {
                        Ok(_) => {
                            match client.session_get(&sess_auth.session_id) {
                                Some(loaded_sess) =>  {
                                    println!("Loaded the logo session: {}", &loaded_sess);
                                },
                                None => println!("Could not load new session")
                            }
                        },
                        Err(err) => println!("Error during session logout {}", err)        
                    }

                },
                Err(err) => println!("Error during session auth {}", err)        
            }
        }, 
        Err(err) => println!("Error while creating new session {}", err)
    }

    match client.upload_document_url(&users[0].user_id.parse()?) {
        Ok(todo_doc_id) => {
            match client.upload_document_completed(&todo_doc_id, "somebucketname", "somekeyval", "somesha") {
                Ok(_) => println!("upload doc completed"),
                Err(err) => println!("upload doc err {}", err)
            }
        },
        Err(err) => println!("Error {}", err)
    }

    let user_documents =  client.get_user_documents(&users[0].user_id);
    for item in &user_documents {
        println!("User document: {}", &item);
    }

    match client.vertex_dot(&users[0].user_id.to_string()) {
        Ok(dot) => println!("dot: {}", dot),
        Err(err) => println!("upload doc err {}", err)
    };
    Ok(())
    }
