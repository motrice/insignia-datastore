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

use log::{info, warn, error};

use uuid::Uuid;

pub mod domain;

use domain::*;


pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, GenericError>;


pub fn new_edge(vertex_a: &Vertex, edge_type: &EdgeType, vertex_b: &Vertex, data: Option<VertexData>) -> Edge {
    Edge {
        vertex_a: vertex_a.to_string(),
        vertex_b: vertex_b.to_string(),
        edge: String::from(format!("{}|{}|{}", edge_type, vertex_a, &vertex_b)),
        data: data
    }
}


pub struct GraphDb {
    pub client: DynamoDbClient
}

impl GraphDb {
    pub fn new() -> GraphDb {
        let region = Region::Custom {
                name: "eu-north-1".to_owned(),
                endpoint: "http://localhost:8000".to_owned(),
        };
        info!("Create dynamodb client");
        GraphDb {
            client: DynamoDbClient::new(region)
        }
    }

    pub fn store_edges(&self, edges: &Vec<Edge>) -> Result<()> {
        // todo retries etc due to documentation

        for itm in edges {
            match self.client.put_item(PutItemInput{
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
                Ok(output) => info!("ok! {:?}", output),
                Err(err) =>  error!("Error {:?}", err)
            };
        }

        Ok(())
    }

    pub fn get_vertex_with_edges(&self, vertex_id: &str) -> Result<Vec<Edge>> {
        // todo retries on error?? 
        let query_key_vertex_a: HashMap<String, AttributeValue> =
            [(String::from(":vertex_a"), AttributeValue{        
                    s:Some(String::from(vertex_id)),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let mut edges_from_vertex_a : Vec<Edge> = match self.client.query(
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
                    error!("Error query{:?}", err);
                    vec![]
                }
        };

        let query_key_vertex_b: HashMap<String, AttributeValue> =
            [(String::from(":vertex_b"), AttributeValue{        
                    s:Some(String::from(vertex_id)),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let mut edges_from_vertex_b : Vec<Edge> = match self.client.query(
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
                    error!("Error query{:?}", err);
                    vec![]
                }
        };

        for item in &edges_from_vertex_a {
            info!("Edge {}  {} {} {:?}", item.vertex_a, item.edge, item.vertex_b, item.data);
        }
        for item in &edges_from_vertex_b {
            info!("GSI Edge {} {} {} {:?}", item.vertex_b, item.edge, item.vertex_a, item.data);
        }
        edges_from_vertex_a.append(&mut edges_from_vertex_b);
        Ok(edges_from_vertex_a)
    }

    pub fn vertex_dot(&self, vertex_id: &str) -> Result<String> {
        let mut render_verticies : HashMap<String, VertexData> = HashMap::new();
        let mut render_edges : Vec<String> = Vec::new();

        let edges = self.get_vertex_with_edges(&vertex_id)?;
        for edge in edges {
            let key = edge.vertex_b;
            let key_a = edge.vertex_a;
            match edge.data {
                Some(data) => { 
                    render_verticies.insert(key.clone(), data.clone());
                },
                None => {
                    render_verticies.insert(key.clone(), VertexData::String(String::from("")));
                }
                
            }; 
            if !render_verticies.contains_key(&key_a) {
                render_verticies.insert(key_a.clone(), VertexData::String(String::from("")));
            }
            render_edges.push(format!(
                r#"{} -> {}"#, 
                &key_a.to_string().replace("-", "_").replace(".", "").replace("+", "").replace("@", ""), 
                &key.replace("-", "_").replace(".", "").replace("+", "").replace("@", "")
            )); 
        }
        Ok(format!(r#"digraph {{ 
            node [shape=plaintext fontname="Sans serif" fontsize="8"];
            {}
            {}
            }}"#, 
            render_verticies.iter().map(|(key, value)|format!(r#" {} [ label=<
        <table border="1" cellborder="0" cellspacing="1" >
        <tr><td align="left"><b>{}</b></td></tr>
        <tr><td align="left">{}</td></tr>
        </table>>];
    "#, &key.replace("-", "_").replace(".", "").replace("+", "").replace("@", ""),
     &key, 
     &value)).collect::<Vec<String>>().join("\n"),
            render_edges.join("\n")))
    }

    pub fn upload_document_url(&self, user_id: &Vertex) -> Result<String> {
        let doc_id = Vertex::Document(Uuid::new_v4().to_hyphenated().to_string());
        let edges = vec![
            new_edge(&user_id, &EdgeType::DocumentOwner, &doc_id, Some(VertexData::String(String::from("some document data")))),
        ];
        self.store_edges(&edges);
        Ok(format!("{}", &doc_id)) // TODO signed url
    }


    pub fn upload_document_completed(&self, doc_id: &str, s3_bucket: &str, s3_key: &str, sha256:&str) -> Result<()> {
        let doc_id : Vertex = doc_id.parse()?;
        let s3_id = Vertex::DocumentS3(String::from(s3_key));
        let checksum_vertex = Vertex::ChecksumSha256(String::from(sha256));
        let edges = vec!{
            new_edge(&doc_id, &EdgeType::DocumentSelf, &doc_id, Some(VertexData::String(String::from("some document data")))),
            new_edge(
                &doc_id, 
                &EdgeType::DocumentS3, 
                &s3_id, 
                Some(VertexData::S3Document(S3Document{bucket: String::from(s3_bucket), key: String::from(s3_key)}))
            ),
            new_edge(&doc_id, &EdgeType::DocumentChecksum, &checksum_vertex, None)
        };
        self.store_edges(&edges)
    }

    pub fn new_user(&self, personal_number: &str, name: &str, given_name: &str, surname: &str, email:Option<&str>, phone:Option<&str>, session_id: Option<&str>) -> Result<String> {
        let user_id = Vertex::User(Uuid::new_v4().to_hyphenated().to_string());
        let pno_vertex = Vertex::PersonalNumber(String::from(personal_number));    
        let mut result = vec!{
            new_edge(
                &user_id, 
                &EdgeType::UserSelf, 
                &user_id, 
                Some(VertexData::UserData(
                    UserData {
                        name: Some(String::from(name)),
                        surname: Some(String::from(surname)),
                        given_name: Some(String::from(given_name))
                    }
                ))
            ),
            new_edge(&user_id, &EdgeType::UserPersonalNumber, &pno_vertex, None),        
        };
        match email {
            Some(email) => {
                let email_vertex = Vertex::Email(String::from(email));
                result.push(new_edge(&user_id, &EdgeType::UserEmail, &email_vertex, None))
            },
            None => {}
        }
        match phone {
            Some(phone) => {
                let phone_vertex = Vertex::Phone(String::from(phone));
                result.push(new_edge(&user_id, &EdgeType::UserPhone, &phone_vertex, None))
            },
            None => {}
        }
        // match session_id {
        //     Some(session_id) => {
        //         let session_vertex = format!("Session-{}", session_id);
        //         result.push(new_edge(&user_id, "session", &session_vertex, None))
        //     },
        //     None => {}
        // }

        match self.store_edges(&result) {
            Ok(_) => Ok(user_id.to_string()),
            Err(err) => Err(err)
        }
    }

    pub fn session_new(&self) -> Result<Session> {
        let session_vertex = Vertex::Session(Uuid::new_v4().to_hyphenated().to_string());
        let now: DateTime<Utc> = Utc::now();
        let created = Some(now.to_rfc3339());

        let edges = vec!{
            new_edge(
                &session_vertex, 
                &EdgeType::SessionSelf, 
                &session_vertex, 
                Some(VertexData::SessionData(SessionData{created: created.clone(), login: None, logout: None, auth_data: None}))
            )
        };
        match self.store_edges(&edges) {
            Ok(_) => Ok(Session{session_id: session_vertex.to_string(), created: created, login: None, logout: None, user: None, auth_data: None}),
            Err(err) => Err(err)
        }
    }

    pub fn session_auth(&self, session_id: &str, user_id: &str, auth_data: &str) -> Result<Session> {
        let now: DateTime<Utc> = Utc::now();
        let login = Some(now.to_rfc3339());
        let session_vertex = session_id.parse()?;

        match self.get_user(user_id) {
            Some(user) => {
                let user_vertex = user_id.parse()?;
                match self.session_get(session_id) {
                    Some(session) => {
                        
                        let edges = vec!{
                            new_edge(
                                &session_vertex, 
                                &EdgeType::SessionUser, 
                                &user_vertex, 
                                Some(VertexData::SessionData(SessionData{login: login.clone(), auth_data: Some(String::from(auth_data)), ..session.session_data()}))
                            )
                        };
                        match self.store_edges(&edges) {
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

    pub fn session_logout(&self, session_id: &str) -> Result<()> {
        let now: DateTime<Utc> = Utc::now();
        let logout = Some(now.to_rfc3339());
        let session_vertex = session_id.parse()?;

        match self.session_get(session_id) {
            Some(session) => {
                match &session.user {
                    Some(user) => {
                        let user_vertex = user.user_id.parse()?;
                        let edges = vec!{
                            new_edge(
                                &session_vertex, 
                                &EdgeType::SessionUser, 
                                &user_vertex, 
                                Some(VertexData::SessionData(SessionData{logout: logout, ..session.session_data()}))
                            )
                        };
                        match self.store_edges(&edges) {
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

    pub fn session_get(&self, session_id: &str) -> Option<Session> {
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

        let edges_from_vertex_a : Vec<Edge> = match self.client.query(
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
                        user = self.get_user(&item.vertex_b);
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
                    _ => info!("Unknown session property {}", &item.vertex_b)
                    
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

    pub fn get_user(&self, user_id: &str) -> Option<User> {
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

        let edges_from_vertex_a : Vec<Edge> = match self.client.query(
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
                    error!("Error query{:?}", err);
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
                    _ => info!("Unknown user property {}", &item.vertex_b)
                    
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

    pub fn get_user_documents(&self, user_id: &str) -> Vec<DocumentReference> {
        let query_key_vertex_a: HashMap<String, AttributeValue> =
            [(String::from(":vertex_a"), AttributeValue{        
                    s:Some(String::from(user_id)),
                    ..Default::default()
                }),
            (String::from(":userdocs"), AttributeValue{        
                    s:Some(String::from("doc_acl_")), 
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let edges_from_vertex_a : Vec<Edge> = match self.client.query(
            QueryInput{
                table_name: String::from("insignia-docs"),
                key_condition_expression: Some(String::from("vertex_a = :vertex_a and begins_with(edge, :userdocs)")),
                expression_attribute_values: Some(query_key_vertex_a),
                .. QueryInput::default()
            }).sync() {
                Ok(res) => {
                    res.items.unwrap_or_else(|| vec![]).into_iter().map(|item| serde_dynamodb::from_hashmap(item).unwrap()).collect()
                },
                Err(err) =>  {
                    error!("Error query{:?}", err);
                    vec![]
                }
        };

        if edges_from_vertex_a.len() == 0 {
            return vec![];
        }

        edges_from_vertex_a.iter().map(|item|DocumentReference{doc_id: item.vertex_b.clone()}).collect::<Vec<DocumentReference>>()
    }



    pub fn get_users_by_personal_number(&self, personal_number: &str) -> Vec<User> {
        let query_key_vertex_b: HashMap<String, AttributeValue> =
            [(String::from(":vertex_b"), AttributeValue{        
                    s:Some(format!("PersonalNumber-{}", personal_number)),
                    ..Default::default()
                })]
            .iter().cloned().collect();

        let edges_from_vertex_b : Vec<Edge> = match self.client.query(
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
                    error!("Error query{:?}", err);
                    vec![]
                }
        };
        edges_from_vertex_b.iter().map(|itm| self.get_user(&itm.vertex_a)).filter(|itm|match itm { Some(v)=> true, None=> false}).map(|itm|itm.unwrap()).collect::<Vec<User>>()
    }

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


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
        fn one_result() -> Result<()> {

            Ok(())
        }
}
