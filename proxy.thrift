namespace go proxy

//Get
struct GetRequest {
    1:string table
    2:binary key
    3:bool forceMaster
}

struct GetResponse {
    1:binary value
    2:i32 code
    3:string msg
}

//Set
struct SetRequest {
    1:string table
    2:binary key
    3:binary value
    4:i32 time
}

struct SetResponse {
    1:i32 code
    2:string msg
}

//Hget
struct HgetRequest {
    1:string table
    2:binary key
    3:binary field
    4:bool forceMaster
}

struct HgetResponse {
    1:binary value
    2:i32 code
    3:string msg
}

//Hset
struct HsetRequest {
    1:string table
    2:binary key
    3:binary field
    4:binary value
}

struct HsetResponse {
    1:i32 code
    2:string msg
}

//Hmget
struct HmgetRequest {
    1:string table
    2:binary key
    3:list<binary> fields
    4:bool forceMaster
}

struct HmgetResponse {
    1:list<binary> value
    2:i32 code
    3:string msg
}

//Hmset
struct HmsetRequest {
    1:string table
    2:binary key
    3:map<string,binary> values
}

struct HmsetResponse {
    1:i32 code
    2:string msg
}

service Service {
    GetResponse getUserFeature(1:GetRequest request)
    SetResponse setUserFeature(1:SetRequest request)
    HgetResponse hgetUserFeature(1:HgetRequest request)
    HsetResponse hsetUserFeature(1:HsetRequest request)
    HmgetResponse hmgetUserFeatures(1:HmgetRequest request)
    HmsetResponse hmsetUserFeatures(1:HmsetRequest request)
}
