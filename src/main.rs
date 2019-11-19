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

enum EdgeType {

}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Edge {
    vertex_a: String,
    vertex_b: String,
    edge: String,
    data: Option<VertexData>
}

/*
impl Edge {
    fn new(vertex_a: &str, vertex_b: &str, edge: &str) -> Edge {
        Edge{
            vertex_a:String::from(vertex_a),
            vertex_b:String::from(vertex_b),
            edge:String::from(format!("{}|{}|{}", edge, vertex_a, vertex_b)),
        }
    }
}
*/

#[derive(Serialize, Deserialize, Clone, Debug)]
enum VertexData {
    S3Document(S3Document),
    String(String),
    UserData(UserData),
    SessionData(SessionData)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct S3Document {
    bucket: String,
    key: String
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct UserData {
    name: Option<String>,
    given_name: Option<String>,
    surname: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SessionData {
    created: Option<String>,
    login: Option<String>,
    logout: Option<String>,
    auth_data: Option<String>
}

struct Organisation {
    id: String,
    org_no: String,
    name: String
}

enum LegalEntity {
    Org(Organisation),
    User(User)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Session {
    session_id: String,
    created: Option<String>,
    login: Option<String>,
    logout: Option<String>,
    auth_data: Option<String>,
    user: Option<User>
}

impl Session {
    fn session_data(&self) -> SessionData {
        SessionData{
            created: self.created.clone(), 
            login: self.login.clone(),
            logout: self.logout.clone(),
            auth_data: self.auth_data.clone()
        }
    }
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Session{{session_id: \"{}\"", self.session_id)?;
        match &self.created {
            Some(s) => write!(f, ", created: Some(\"{}\")", s)?,
            None => write!(f, ", created: None")?
        };
        match &self.login {
            Some(s) => write!(f, ", login: Some(\"{}\")", s)?,
            None => write!(f, ", login: None")?
        };
        match &self.logout {
            Some(s) => write!(f, ", logout: Some(\"{}\")", s)?,
            None => write!(f, ", logout: None")?
        };
        match &self.auth_data {
            Some(s) => write!(f, ", auth_data: Some(\"{}\")", s)?,
            None => write!(f, ", auth_data: None")?
        };
        match &self.user {
            Some(s) => write!(f, ", user: Some(\"{}\")", s)?,
            None => write!(f, ", user: None")?
        };
        write!(f, "}}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct User {
    user_id: String,
    name: Option<String>,
    given_name: Option<String>,
    surname: Option<String>,
    personal_number: Option<String>,
    email: Option<String>,
    phone: Option<String>    
}

impl std::fmt::Display for User {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "User{{user_id: \"{}\"", self.user_id)?;
        match &self.name {
            Some(s) => write!(f, ", name: Some(\"{}\")", s)?,
            None => write!(f, ", name: None")?
        };
        match &self.given_name {
            Some(s) => write!(f, ", given_name: Some(\"{}\")", s)?,
            None => write!(f, ", given_name: None")?
        };
        match &self.surname {
            Some(s) => write!(f, ", surname: Some(\"{}\")", s)?,
            None => write!(f, ", surname: None")?
        };
        match &self.personal_number {
            Some(s) => write!(f, ", personal_number: Some(\"{}\")", s)?,
            None => write!(f, ", personal_number: None")?
        };
        match &self.email {
            Some(s) => write!(f, ", email: Some(\"{}\")", s)?,
            None => write!(f, ", email: None")?
        };
        match &self.phone {
            Some(s) => write!(f, ", phone: Some(\"{}\")", s)?,
            None => write!(f, ", phone: None")?
        };
        write!(f, "}}")
    }
}

struct Document {
    doc_id: String,
    s3: S3Document,
    owners: Vec<LegalEntity>,
    signatures: Vec<LegalEntity>,
    signature_reqs: Vec<LegalEntity>    
}

fn new_edge(vertex_a: &str, edge_type: &str, vertex_b: &str, data: Option<VertexData>) -> Edge {
    Edge {
        vertex_a: String::from(vertex_a),
        vertex_b: String::from(vertex_b),
        edge: String::from(format!("{}|{}|{}", edge_type, vertex_a, &vertex_b)),
        data: data
    }
}

fn store_edges(client: &DynamoDbClient, edges: &Vec<Edge>) -> Result<()> {
    // todo retries etc due to documentation

    for itm in edges {
        match client.put_item(PutItemInput{
            condition_expression: None,
            conditional_operator: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            item: serde_dynamodb::to_hashmap(&itm).unwrap(),
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            table_name: String::from("insignia-docs")


        }).sync() {
            Ok(output) => println!("ok! {:?}", output),
            Err(err) =>  println!("Error {:?}", err)
        };
    }

    Ok(())
}

fn upload_document(user_id: &str, s3_bucket: &str, s3_key: &str, sha256:&str) -> Vec<Edge> {
    let doc_id = format!("Document-{}", &Uuid::new_v4().to_hyphenated().to_string());
    let s3_id = format!("S3-{}", String::from(s3_key));
    let checksum_vertex = format!("sha256-{}", String::from(sha256));
    vec!{
        new_edge(&doc_id, "self", &doc_id, Some(VertexData::String(String::from("some document data")))),
        new_edge(&user_id, "owner", &doc_id, Some(VertexData::String(String::from("some document data")))),
        new_edge(
            &doc_id, 
            "s3", 
            &s3_id, 
            Some(VertexData::S3Document(S3Document{bucket: String::from(s3_bucket), key: String::from(s3_key)}))
        ),
        new_edge(&doc_id, "checksum", &checksum_vertex, None)
    }
}

fn new_user(personal_number: &str, name: &str, given_name: &str, surname: &str, email:Option<&str>, phone:Option<&str>, session_id: Option<&str>) -> Vec<Edge> {
    let user_id = format!("User-{}", &Uuid::new_v4().to_hyphenated().to_string());
    let pno_vertex = format!("PersonalNumber-{}", personal_number);
    //let pno_vertex = format!("Email-{}", email);
    
    let mut result = vec!{
        new_edge(
            &user_id, 
            "usr_self", 
            &user_id, 
            Some(VertexData::UserData(
                UserData {
                    name: Some(String::from(name)),
                    surname: Some(String::from(surname)),
                    given_name: Some(String::from(given_name))
                }
            ))
        ),
        new_edge(&user_id, "usr_personal_number", &pno_vertex, None),        
    };
    match email {
        Some(email) => {
            let email_vertex = format!("Email-{}", email);
            result.push(new_edge(&user_id, "usr_email", &email_vertex, None))
        },
        None => {}
    }
    match phone {
        Some(phone) => {
            let phone_vertex = format!("Phone-{}", phone);
            result.push(new_edge(&user_id, "usr_phone", &phone_vertex, None))
        },
        None => {}
    }
    match session_id {
        Some(session_id) => {
            let session_vertex = format!("Session-{}", session_id);
            result.push(new_edge(&user_id, "session", &session_vertex, None))
        },
        None => {}
    }
    result
}

fn session_new(client: &DynamoDbClient) -> Result<Session> {
    let session_vertex = format!("Session-{}", &Uuid::new_v4().to_hyphenated().to_string());
    let now: DateTime<Utc> = Utc::now();
    let created = Some(now.to_rfc3339());

    let edges = vec!{
        new_edge(
            &session_vertex, 
            "session_self", 
            &session_vertex, 
            Some(VertexData::SessionData(SessionData{created: created.clone(), login: None, logout: None, auth_data: None}))
        )
    };
    match store_edges(&client, &edges) {
        Ok(_) => Ok(Session{session_id: session_vertex, created: created, login: None, logout: None, user: None, auth_data: None}),
        Err(err) => Err(err)
    }
}

fn session_auth(client: &DynamoDbClient, session_id: &str, user_id: &str, auth_data: &str) -> Result<Session> {
    let now: DateTime<Utc> = Utc::now();
    let login = Some(now.to_rfc3339());

    match get_user(&client, user_id) {
        Some(user) => {
            match session_get(&client, session_id) {
                Some(session) => {
                    
                    let edges = vec!{
                        new_edge(
                            &session_id, 
                            "session_user", 
                            &user_id, 
                            Some(VertexData::SessionData(SessionData{login: login.clone(), auth_data: Some(String::from(auth_data)), ..session.session_data()}))
                        )
                    };
                    match store_edges(&client, &edges) {
                        Ok(_) => Ok(Session{login: login, user: Some(user), ..session}),
                        Err(err) => Err(err)
                    }

                },
                None => Err(GenericError::from("Invalid session_id"))
            }
        },
        None => Err(GenericError::from("Invalid user_id"))
    }
}

fn session_logout(client: &DynamoDbClient, session_id: &str) -> Result<()> {
    let now: DateTime<Utc> = Utc::now();
    let logout = Some(now.to_rfc3339());

    match session_get(&client, session_id) {
        Some(session) => {
            match &session.user {
                Some(user) => {
                    let edges = vec!{
                        new_edge(
                            &session.session_id, 
                            "session_user", 
                            &user.user_id, 
                            Some(VertexData::SessionData(SessionData{logout: logout, ..session.session_data()}))
                        )
                    };
                    match store_edges(&client, &edges) {
                        Ok(_) => Ok(()),
                        Err(err) => Err(err)
                    }
                },
                None => Err(GenericError::from("No user in session, maybe not logged in"))

            }
        },
        None => Err(GenericError::from("Invalid session_id"))
    }
}

fn session_get(client: &DynamoDbClient, session_id: &str) -> Option<Session> {
    let query_key_vertex_a: HashMap<String, AttributeValue> =
        [(String::from(":vertex_a"), AttributeValue{        
                s:Some(String::from(session_id)),
                ..Default::default()
            }),
        (String::from(":userdata_prefix"), AttributeValue{        
                s:Some(String::from("session_")),
                ..Default::default()
            })]
        .iter().cloned().collect();

    let edges_from_vertex_a : Vec<Edge> = match client.query(
        QueryInput{
            table_name: String::from("insignia-docs"),
            key_condition_expression: Some(String::from("vertex_a = :vertex_a and begins_with(edge, :userdata_prefix)")),
            expression_attribute_values: Some(query_key_vertex_a),
            .. QueryInput::default()
        }).sync() {
            Ok(res) => {
                res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
            },
            Err(err) =>  {
                vec![]
            }
    };

    if edges_from_vertex_a.len() == 0 {
        return None;
    }

    let mut created : Option<String> = None;
    let mut login: Option<String> = None;
    let mut logout: Option<String> = None;
    let mut auth_data: Option<String> = None;
    let mut user : Option<User> = None;

    for item in &edges_from_vertex_a {
        let splitted : Vec<&str> = item.vertex_b.split("-").collect();
        if splitted.len()>1 {
            match splitted[0] {
                "User" => {
                    match &item.data {
                        Some(data) => match data {
                            VertexData::SessionData(session_data) => {
                                login = session_data.login.clone();
                                auth_data = session_data.auth_data.clone();
                                logout = session_data.logout.clone();
                                ()
                            },
                            _ => () 
                        },
                        None => ()
                    }
                    user = get_user(&client, &item.vertex_b);
                },
                "Session" => {
                    match &item.data {
                        Some(data) => match data {
                            VertexData::SessionData(session_data) => {
                                created = session_data.created.clone();
                                ()
                            },
                            _ => () 
                        },
                        None => ()
                    }
                },
                _ => println!("Unknown session property {}", &item.vertex_b)
                
            }
        }
    }

    Some(Session{
        session_id: String::from(session_id),
        created: created,
        login: login,
        logout: logout,
        auth_data: auth_data,
        user: user
    })

}

fn get_user(client: &DynamoDbClient, user_id: &str) -> Option<User> {
    let query_key_vertex_a: HashMap<String, AttributeValue> =
        [(String::from(":vertex_a"), AttributeValue{        
                s:Some(String::from(user_id)),
                ..Default::default()
            }),
        (String::from(":userdata_prefix"), AttributeValue{        
                s:Some(String::from("usr_")),
                ..Default::default()
            })]
        .iter().cloned().collect();

    let edges_from_vertex_a : Vec<Edge> = match client.query(
        QueryInput{
            table_name: String::from("insignia-docs"),
            key_condition_expression: Some(String::from("vertex_a = :vertex_a and begins_with(edge, :userdata_prefix)")),
            expression_attribute_values: Some(query_key_vertex_a),
            .. QueryInput::default()
        }).sync() {
            Ok(res) => {
                res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
            },
            Err(err) =>  {
                println!("Error query{:?}", err);
                vec![]
            }
    };

    if edges_from_vertex_a.len() == 0 {
        return None;
    }

    let mut name : Option<String> = None;
    let mut given_name: Option<String> = None;
    let mut surname: Option<String> = None;
    let mut personal_number: Option<String> = None;
    let mut email: Option<String> = None;
    let mut phone: Option<String> = None;

    for item in &edges_from_vertex_a {
        let splitted : Vec<&str> = item.vertex_b.split("-").collect();
        if splitted.len()>1 {
            match splitted[0] {
                "Email" => {
                    email = Some(splitted[1..].join("-"));
                },
                "PersonalNumber" => {
                    personal_number = Some(splitted[1..].join("-"));
                },
                "Phone" => {
                    phone = Some(splitted[1..].join("-"));
                },
                "User" => {
                    match &item.data {
                        Some(data) => match data {
                            VertexData::UserData(user_data) => {
                                name = user_data.name.clone();
                                given_name = user_data.given_name.clone();
                                surname = user_data.surname.clone();
                                ()
                            },
                            _ => () 
                        },
                        None => ()
                    }
                },
                _ => println!("Unknown user property {}", &item.vertex_b)
                
            }
        }
    }

    Some(User{
        user_id: String::from(user_id),
        name: name,
        given_name: given_name,
        surname: surname,
        personal_number: personal_number,
        email: email,
        phone: phone
    })
}

fn get_users_by_personal_number(client: &DynamoDbClient, personal_number: &str) -> Vec<User> {
    let query_key_vertex_b: HashMap<String, AttributeValue> =
        [(String::from(":vertex_b"), AttributeValue{        
                s:Some(format!("PersonalNumber-{}", personal_number)),
                ..Default::default()
            })]
        .iter().cloned().collect();

    let edges_from_vertex_b : Vec<Edge> = match client.query(
        QueryInput{
            table_name: String::from("insignia-docs"),
            key_condition_expression: Some(String::from("vertex_b = :vertex_b")),
            index_name: Some(String::from("index-vertex_b_edges")),
            expression_attribute_values: Some(query_key_vertex_b),
            .. QueryInput::default()
        }).sync() {
            Ok(res) => {
                res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
            },
            Err(err) =>  {
                println!("Error query{:?}", err);
                vec![]
            }
    };
    edges_from_vertex_b.iter().map(|itm| get_user(&client, &itm.vertex_a)).filter(|itm|match itm { Some(v)=> true, None=> false}).map(|itm|itm.unwrap()).collect::<Vec<User>>()
}

fn get_vertex_with_edges(client: &DynamoDbClient, vertex_id: &str) -> Result<Vec<Edge>> {
    let query_key_vertex_a: HashMap<String, AttributeValue> =
        [(String::from(":vertex_a"), AttributeValue{        
                s:Some(String::from(vertex_id)),
                ..Default::default()
            })]
        .iter().cloned().collect();

    let mut edges_from_vertex_a : Vec<Edge> = match client.query(
        QueryInput{
            table_name: String::from("insignia-docs"),
            key_condition_expression: Some(String::from("vertex_a = :vertex_a")),
            expression_attribute_values: Some(query_key_vertex_a),
            .. QueryInput::default()
        }).sync() {
            Ok(res) => {
                res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
            },
            Err(err) =>  {
                println!("Error query{:?}", err);
                vec![]
            }
    };

    let query_key_vertex_b: HashMap<String, AttributeValue> =
        [(String::from(":vertex_b"), AttributeValue{        
                s:Some(String::from(vertex_id)),
                ..Default::default()
            })]
        .iter().cloned().collect();

    let mut edges_from_vertex_b : Vec<Edge> = match client.query(
        QueryInput{
            table_name: String::from("insignia-docs"),
            key_condition_expression: Some(String::from("vertex_b = :vertex_b")),
            index_name: Some(String::from("index-vertex_b_edges")),
            expression_attribute_values: Some(query_key_vertex_b),
            .. QueryInput::default()
        }).sync() {
            Ok(res) => {
                res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
            },
            Err(err) =>  {
                println!("Error query{:?}", err);
                vec![]
            }
    };

    for item in &edges_from_vertex_a {
        println!("Edge {}  {} {} {:?}", item.vertex_a, item.edge, item.vertex_b, item.data);
    }
    for item in &edges_from_vertex_b {
        println!("GSI Edge {} {} {} {:?}", item.vertex_b, item.edge, item.vertex_a, item.data);
    }
    edges_from_vertex_a.append(&mut edges_from_vertex_b);
    Ok(edges_from_vertex_a)
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

doc-nnnn    doc-nnnn        "title: String, checksum: String, created, modified"
doc-nnnn    s3-ref
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


type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;



#[tokio::main]
pub async fn main() -> Result<()> {

    println!("Hello, world!");

    let region = Region::Custom {
        name: "eu-north-1".to_owned(),
        endpoint: "http://localhost:8000".to_owned(),
    };

    let client = DynamoDbClient::new(region);

    match client.delete_table(DeleteTableInput{table_name: String::from("bm-test-table")}).sync() {
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

doc-nnnn    doc-nnnn        "title: String, checksum: String, created, modified"
doc-nnnn    s3-ref
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
    match client.create_table(CreateTableInput{
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

    // mock some data TODO remove
    // Some data structure.
    let mut itms = vec![
        
    ];
    let mut user1 = new_user("191212121212", "Tolvan Tolvansson", "Tolvan", "Tolvansson", Some("tolvan.tolvansson@motrice.se"), Some("+46733414983"), None);
    let user1_vertex = user1.iter().filter(|itm| {itm.edge.starts_with("usr_self|")}).collect::<Vec<&Edge>>().pop().unwrap().vertex_a.clone();
    itms.append(&mut user1);
    itms.append(&mut upload_document(&user1_vertex, &"bucket", &"key", &"checksum"));
    
    for itm in itms {
        match client.put_item(PutItemInput{
            condition_expression: None,
            conditional_operator: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            expected: None,
            item: serde_dynamodb::to_hashmap(&itm).unwrap(),
    //        item: "{\"vertex_a\": {\"S\": \"Doc-1234567\"}, \"node_b\": {\"S\": \"owner\"}, \"created\": {\"N\": \"1234\"}}".into() ,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            table_name: String::from("insignia-docs")


        }).sync() {
            Ok(output) => println!("ok! {:?}", output),
            Err(err) =>  println!("Error {:?}", err)
        };
    }


    let scan_edges : Vec<Edge> = match client.scan(ScanInput{
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
        let edges = get_vertex_with_edges(&client, &item.vertex_a).unwrap();
        println!("Vertex: {}", &item.vertex_a);
        for edge in &edges {
            //println!("    Edge {} {} {} {:?}", edge.vertex_b, edge.edge, edge.vertex_a, edge.data);
            println!("    Edge {} {:?}", edge.edge, edge.data);
            
        }
    }

    let users = get_users_by_personal_number(&client, "191212121212");
    for item in &users {
        println!("Tolvan user: {}", item);
    }

    match session_new(&client) {
        Ok(session) => {
            println!("new session:            {}", &session);
            match session_get(&client, &session.session_id) {
                Some(loaded_sess) =>  {
                    println!("Loaded the new session: {}", &loaded_sess);
                },
                None => println!("Could not load new session")
            }
            match session_auth(&client, &session.session_id, &users[0].user_id, "logged in from ip 123.456.7.8") {
                Ok(sess_auth) =>  {
                    println!("auth session:            {}", &session);
                    match session_get(&client, &sess_auth.session_id) {
                        Some(loaded_sess) =>  {
                            println!("Loaded the auth session: {}", &loaded_sess);
                        },
                        None => println!("Could not load new session")
                    }

                    match session_logout(&client, &sess_auth.session_id) {
                        Ok(_) => {
                            match session_get(&client, &sess_auth.session_id) {
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
    
    Ok(())
}
