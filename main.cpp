#include <iostream>
#include "mysql.h"
//creds
#include "Credentials.h"
//poco
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAcceptor.h>
#include <Poco/Net/SocketReactor.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Runnable.h>
#include <Poco/UUIDGenerator.h>
#include <iostream>
#include <string>
#include <cstring>
#include "lib/json/include/nlohmann/json.hpp"

using namespace std;

MYSQL* conn;

void Log(string s){
    std::cout << "[main] " +s << std::endl;
}

void ConnectToDB() {

    MYSQL_RES *result;
    MYSQL_ROW row;

    //via Credentials.h
    Credentials creds = Credentials();

    const char* server = creds.server;
    const char* user = creds.user;
    const char* password = creds.password;
    const char* database = creds.database;


    conn = mysql_init(NULL);

    if (!mysql_real_connect(conn, server, user, password, database, 0, NULL, 0)) {
        fprintf(stderr, "%s\n", mysql_error(conn));
    }

    if (mysql_query(conn, "show tables")) {
        fprintf(stderr, "%s\n", mysql_error(conn));
    }

    result = mysql_use_result(conn);

    //remove this when we are comfortable with the SQL implementation
    printf("MySQL Tables in mysql database:\n");
    while ((row = mysql_fetch_row(result)) != NULL)
        printf("%s \n", row[0]);

    mysql_free_result(result);
    //mysql_close(conn);
}



// Assumes 'using namespace nlohmann;' is declared if you're not specifying 'nlohmann::json' explicitly.

    std::vector<nlohmann::json> ConvertResultsToJson(MYSQL_RES *result) {
        Log("converting results to json...");
        std::vector<nlohmann::json> jsonResults;
        MYSQL_ROW row;
        MYSQL_FIELD *fields;
        unsigned int num_fields;

        num_fields = mysql_num_fields(result);
        fields = mysql_fetch_fields(result);

        while ((row = mysql_fetch_row(result))) {
            nlohmann::json rowJson;
            for(unsigned int i = 0; i < num_fields; i++) {
                if (row[i]) { // Check if the column value is not NULL
                    rowJson[fields[i].name] = row[i];
                } else { // If NULL, you might want to set it to a JSON null
                    rowJson[fields[i].name] = nullptr;
                }
            }
            Log("[ConvertResultsToJson] " + rowJson.dump());
            jsonResults.push_back(rowJson);
        }
        return jsonResults;
    }

    nlohmann::json CheckTableTypes(string tableName) {
        Log("checking table types for: " + tableName);
        nlohmann::json resultJson;

        // Create the SQL statement
        string sql = "DESCRIBE " + tableName + ";";
        const char* sqlStatement = sql.c_str();


        // Execute the SQL statement
        if (mysql_query(conn, sqlStatement) != 0) {
            std::cerr << "Error executing SQL statement: " << mysql_error(conn) << std::endl;
            return resultJson;
        }

        // Fetch the result
        MYSQL_RES* result = mysql_store_result(conn);
        if (result == nullptr) {
            std::cerr << "Error retrieving result set: " << mysql_error(conn) << std::endl;
            return resultJson;
        }

        // Loop through each row in the result set
        while (MYSQL_ROW row = mysql_fetch_row(result)) {
            const char* columnName = row[0];
            const char* columnType = row[1];
            Log(string(columnName) + " : " + string(columnType));
            // Add the column to the JSON object
            resultJson[columnName] = columnType;
        }

        // Free the result set
        mysql_free_result(result);

        return resultJson;
    }

// Assume `db` is your MYSQL* connection established elsewhere in your code.

    std::vector<nlohmann::json> SelectTop(std::string tableName) {
        std::vector<nlohmann::json> results;

        // Construct the query
        std::string query = "SELECT * FROM " + tableName + " LIMIT 25;";

        Log("query: " + query);

        int status = mysql_query(conn, query.c_str());
        MYSQL_RES * queryResult;
        if (status == 0){
            queryResult = mysql_store_result(conn);
            if(queryResult){
                //there are rows
                Log("Found: " + to_string(mysql_num_rows(queryResult)) + " rows");
                results = ConvertResultsToJson(queryResult);
            }
            else{
                Log("No rows found");
            }
        }
        else{
            Log("Query failed");
        }

        // Assuming you have a function to convert MYSQL_RES* to a vector of nlohmann::json

        //debugging;
        Log("results: ");
        for(int i = 0; i < results.size(); i++){
            nlohmann::json element = results[i];
            Log(element.dump());
        }

        // Cleanup
        mysql_free_result(queryResult);

        return results;
    }

    std::vector<nlohmann::json> SelectWhere(std::string tableName, nlohmann::json filters) {
        Log("filters: " + filters.dump());

        std::vector<nlohmann::json> results;

        //get column names and types from table
        //so we can strongly type all of the data when we write to the db
        nlohmann::json tableTypes = CheckTableTypes(tableName);
        Log("table types: " + tableTypes.dump());
        vector<string> table_keys = {};
        for (auto& [key, value] : tableTypes.items()){
            table_keys.push_back(key);
        }

        // Bind parameters
        int param_index = 0;
        //param count = number of key:value pairs in JSON obj
        MYSQL_BIND params[filters.size()]; //these are the actual params with the actual data
        memset(params, 0, sizeof(params));

        vector<char*> malloc_string_buffers; //vec of pointers of char* type
        vector<int*> malloc_int_buffers;
        vector<float*> malloc_float_buffers;

        string column_part = "";
        int columnKey_index  = 0;
        for (auto& [key, value] : tableTypes.items()) {
            if(columnKey_index == table_keys.size() - 1){//last
                column_part += key;
            }
            else{
                column_part += key;
                column_part += ", ";
            }
            columnKey_index ++;
        }
        // Begin constructing the query
        std::string query = "SELECT " + column_part + " FROM " + tableName + " WHERE ";
        for (auto& [key, value] : filters.items()) {
            if (param_index > 0) query += " AND ";
            query += key + " = ?";
            //type checking/casting
            //get the column's type in the sql db
            string target_type = string(tableTypes[key]);
            if(target_type == "INT"){
                Log("target type = INT");
                params[param_index].buffer_type = MYSQL_TYPE_INT24;
                int v = static_cast<int>(value);
                int* buffer = new int(v);
                params[param_index].buffer = buffer; // Use the original string value
                params[param_index].buffer_length = sizeof(buffer);
                malloc_int_buffers.push_back(buffer);
            }else if(target_type == "FLOAT"){
                Log("target type = FLOAT");
                params[param_index].buffer_type = MYSQL_TYPE_FLOAT;
                float v = static_cast<float>(value);
                float* buffer = new float(v);
                params[param_index].buffer = buffer; // Use the original string value
                params[param_index].buffer_length = sizeof(buffer);
                malloc_float_buffers.push_back(buffer);
            }else{
                Log("target type = string");
                params[param_index].buffer_type = MYSQL_TYPE_STRING;
                // Manually allocate memory and copy the string data.
                string valueStr = string(value);
                char* buffer = new char[valueStr.length() + 1];
                std::strcpy(buffer, valueStr.c_str());
                params[param_index].buffer = buffer; // Use the original string value
                params[param_index].buffer_length = valueStr.length();
                malloc_string_buffers.push_back(buffer);//keep track of what we have allocated
            }
            param_index++;
        }
        query += ";";

        Log("[SelectWhere] query: " + query);


        MYSQL_STMT *stmt = mysql_stmt_init(conn);
        if (!stmt) {
            std::cerr << "mysql_stmt_init(), out of memory\n";
            return results;
        }

        int rc = mysql_stmt_prepare(stmt, query.c_str(), query.length());
        if (rc != 0) {
            std::cerr << "mysql_stmt_prepare(), failed: " << mysql_stmt_error(stmt) << "\n";
            return results;
        }

        rc = mysql_stmt_bind_param(stmt, params);
        if (rc != 0) {
            std::cerr << "mysql_stmt_bind_param(), failed\n";
            return results;
        }

        // Execute the query
        rc = mysql_stmt_execute(stmt);
        if (rc != 0) {
            std::cerr << "mysql_stmt_execute(), failed: " << mysql_stmt_error(stmt) << "\n";
            return results;
        }

        // this only contains metadata we need to fetch the row data after we do this part
        /* Fetch result set meta information */
        MYSQL_RES* prepare_meta_result = mysql_stmt_result_metadata(stmt);
        if (!prepare_meta_result)
        {
            fprintf(stderr,
                    " mysql_stmt_result_metadata(), \
           returned no meta information\n");
            fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
            exit(0);
        }

        //create new buffers to bind the results to

        unsigned int num_fields = mysql_num_fields(prepare_meta_result);
        MYSQL_FIELD* fields = mysql_fetch_fields(prepare_meta_result);
        MYSQL_BIND* bind = new MYSQL_BIND[num_fields];
        memset(bind, 0, sizeof(MYSQL_BIND) * num_fields);

        // Setup buffer to store results
        std::vector<char*> rowBuffers(num_fields);
        std::vector<unsigned long> lengths(num_fields);
        for (unsigned int i = 0; i < num_fields; i++) {
            rowBuffers[i] = new char[256]; // Assuming a max field length, adjust as necessary
            bind[i].buffer_type = MYSQL_TYPE_STRING;
            bind[i].buffer = rowBuffers[i];
            bind[i].buffer_length = 256;
            bind[i].length = &lengths[i];
        }

        if (mysql_stmt_bind_result(stmt, bind)) {
            std::cerr << "mysql_stmt_bind_result() failed: " << mysql_stmt_error(stmt) << std::endl;
            mysql_stmt_close(stmt);
            mysql_free_result(prepare_meta_result);
            return results;
        }

        // Fetch and populate JSON
        while (!mysql_stmt_fetch(stmt)) {
            nlohmann::json rowJson;
            for (unsigned int i = 0; i < num_fields; i++) {
                rowJson[fields[i].name] = std::string(rowBuffers[i], *bind[i].length);
            }
            results.push_back(rowJson);
        }

        // Cleanup
        for (unsigned int i = 0; i < num_fields; i++) {
            delete[] rowBuffers[i];
        }
        delete[] bind;
        mysql_free_result(prepare_meta_result);
        mysql_stmt_close(stmt);

        //very important to free the memory since we used new to create it
        //do this for all types that i need to handle
        for (char* buffer : malloc_string_buffers) {
            delete[] buffer;
        }
        for (int* buffer : malloc_int_buffers) {
            delete[] buffer;
        }
        for (float* buffer : malloc_float_buffers) {
            delete[] buffer;
        }

        return results;
    }


    //you must include primary key(s) for this to work
    //vector<json> = list of db rows
    std::string ReplaceMany(std::string tableName, std::vector<nlohmann::json> filters) {

        //this is stuff i copied from the c api. i need to covert it to use my logic

        if (filters.empty()) {
            return "No records to replace";
        }

        //get column names and types from table
        //so we can strongly type all of the data when we write to the db
        nlohmann::json tableTypes = CheckTableTypes(tableName);
        Log("table types: " + tableTypes.dump());

        string column_part = "(";
        string update_part("");

        vector<string> table_keys = {};
        for (auto& [key, value] : tableTypes.items()){
            table_keys.push_back(key);
        }

        int columnKey_index  = 0;
        for (auto& [key, value] : tableTypes.items()) {
            if(columnKey_index == table_keys.size() - 1){//last
                column_part += key;
                column_part += ")";
                update_part += key;
                update_part += " = VALUES(" + key + ") \n";
            }
            else{
                column_part += key;
                column_part += ", ";
                update_part += key;
                update_part += " = VALUES(" + key + "), \n";
            }
            columnKey_index ++;
        }

        // variable declarations
        MYSQL_STMT *stmt = mysql_stmt_init(conn);
        //param count = rows * elements in each row
        MYSQL_BIND params[filters.size() * filters[0].size()]; //these are the actual params with the actual data
        memset(params, 0, sizeof(params));
        int rc;

        string values_part("");

        vector<char*> malloc_string_buffers; //vec of pointers of char* type
        vector<int*> malloc_int_buffers;
        vector<float*> malloc_float_buffers;
        int param_index = 0;
        //build a prepared query
        for(int i = 0; i < filters.size(); i++){//foreach row of data
            columnKey_index = 0;
            string value_row = "(";
            for(auto &[key, value] : filters[i].items()){//for each item in the row
                //filter[i] = row
                //filter[i][key] = row element value

                //type checking/casting
                //get the column's type in the sql db
                string target_type = string(tableTypes[key]);
                if(target_type == "INT"){
                    Log("target type = INT");
                    params[param_index].buffer_type = MYSQL_TYPE_INT24;
                    int v = static_cast<int>(value);
                    int* buffer = new int(v);
                    params[param_index].buffer = buffer; // Use the original string value
                    params[param_index].buffer_length = sizeof(buffer);
                    malloc_int_buffers.push_back(buffer);
                }else if(target_type == "FLOAT"){
                    Log("target type = FLOAT");
                    params[param_index].buffer_type = MYSQL_TYPE_FLOAT;
                    float v = static_cast<float>(value);
                    float* buffer = new float(v);
                    params[param_index].buffer = buffer; // Use the original string value
                    params[param_index].buffer_length = sizeof(buffer);
                    malloc_float_buffers.push_back(buffer);
                }else{
                    Log("target type = string");
                    params[param_index].buffer_type = MYSQL_TYPE_STRING;
                    // Manually allocate memory and copy the string data.
                    string valueStr = string(value);
                    char* buffer = new char[valueStr.length() + 1];
                    std::strcpy(buffer, valueStr.c_str());
                    params[param_index].buffer = buffer; // Use the original string value
                    params[param_index].buffer_length = valueStr.length();
                    malloc_string_buffers.push_back(buffer);//keep track of what we have allocated
                }

                //end the row
                if(columnKey_index == table_keys.size() - 1){
                    value_row += "?";
                    value_row += string(")");
                    columnKey_index = 0; //reset
                }
                else{
                    value_row += "?";
                    value_row += ", ";
                }
                param_index++;
                columnKey_index++;
            }//<-- for each item in the row
            values_part += "\n";
            if(i == filters.size() -1){//last row
                values_part += value_row;
            }
            else{
                values_part += value_row + ",";
            }
        }


        string prepared_query = "INSERT INTO " + tableName + " " + column_part + " \n"
                                                                                 "VALUES"
                                + values_part + "\nON DUPLICATE KEY UPDATE \n" + update_part + ";";
        Log(prepared_query);

        //bind the real data
        /* mysql just looks for question marks and then replaces them in the order that they
         * appear in the query string with the bound params by index. The MYSQL_BIND type is static so I need to know
         * the size beforehand. */
        //run the prepared query
        const char* c_string = prepared_query.c_str();
        if (mysql_stmt_prepare(stmt, c_string, strlen(c_string))) {
            mysql_stmt_close(stmt);
            mysql_close(conn);
            Log("Error on prepare");
        }

        rc = mysql_stmt_bind_param(stmt, params);
        if (rc != 0) {
            fprintf(stderr, "Error in mysql_stmt_bind_param(): %s\n", mysql_stmt_error(stmt));
            // Additional error handling such as cleanup and exit
            mysql_stmt_close(stmt);
            mysql_close(conn);
            exit(1); // or return a specific error code
        }

        rc = mysql_stmt_execute(stmt);
        Log("executing statement...");
        if (rc != 0) {
            fprintf(stderr, "Error in mysql_stmt_execute(): %s\n", mysql_stmt_error(stmt));
            // Additional error handling such as cleanup and exit
            mysql_stmt_close(stmt);
            mysql_close(conn);
            exit(1); // or return a specific error code
        }

        Log("Closing connection");
        rc = mysql_stmt_close(stmt);
        if (rc != 0) { // This check depends on MySQL version and documentation; traditionally, it returns an int.
            fprintf(stderr, "Error in mysql_stmt_close(): %s\n", mysql_stmt_error(stmt));
            // Since mysql_stmt_close() failed, you might not need additional action,
            // but consider logging this issue.
            mysql_close(conn);
            exit(1); // or return a specific error code
        }

        //very important to free the memory since we used new to create it
        //do this for all types that i need to handle
        for (char* buffer : malloc_string_buffers) {
            delete[] buffer;
        }
        for (int* buffer : malloc_int_buffers) {
            delete[] buffer;
        }
        for (float* buffer : malloc_float_buffers) {
            delete[] buffer;
        }
        return string("records inserted successfully");
    }

//takes in a json message and replies
nlohmann::json ProcessCommand(nlohmann::json json_obj){
    string command= json_obj["command"];
    nlohmann::json reply = {};

    //force lower case
    std::transform(command.begin(), command.end(), command.begin(),
                   [](unsigned char c) -> unsigned char { return std::tolower(c); });

    reply["status"] = "received";

    if(command == "replace many"){
        nlohmann::json data = json_obj["data"];
        string tableName = json_obj["table name"];
        string result = ReplaceMany(tableName, data);
        reply["results"] = result;
    }
    else if(command == "select top"){
        string tableName = json_obj["table name"];
        vector<nlohmann::json> results = SelectTop(tableName);
        reply["results"] = results;
    }
    else if(command == "select where"){
        nlohmann::json data = json_obj["data"];
        string tableName = json_obj["table name"];
        vector<nlohmann::json> results = SelectWhere(tableName, data);
        reply["results"] = results;
    }

    return reply;
}

class JSONConnection : public Poco::Net::TCPServerConnection {
public:
    JSONConnection(const Poco::Net::StreamSocket &socket)
            : Poco::Net::TCPServerConnection(socket) {}

    void run() override {//loop exits when the client closes the connection.
        try {
            Poco::Net::StreamSocket &ss = socket();
            char buffer[1024 * 8];
            int n = ss.receiveBytes(buffer, sizeof(buffer)); //number of bytes received
            Log("bytes received: " + to_string(n));
            while (n > 0) {
                std::string buff_str(buffer, n);
                // Parse JSON
                try {
                    nlohmann::json json_obj = nlohmann::json::parse(buff_str);
                    nlohmann::json reply = ProcessCommand(json_obj);
                    Log("reply:");
                    string reply_str = reply.dump();
                    reply_str += "\n";
                    Log(reply_str);
                    //send reply
                    try {
                        Log("responding to client");
                        ss.sendBytes(reply_str.data(), reply_str.size());
                    } catch (Poco::Exception& e) {
                        std::cerr << "Error sending response: " << e.displayText() << std::endl;
                    }
                }
                catch (exception) {
                    Log("There was an error");
                    Log("json size: ");
                    Log(to_string(n));
                }
                //this is a blocking call
                //n = 0 if ss.receiveBytes gets nothing
                n = ss.receiveBytes(buffer, sizeof(buffer));
                //get a new message or more of the same one
            }
        } catch (Poco::Exception &e) {
            std::cerr << "Connection error: " << e.displayText() << std::endl;
        }
    }//loop exits when the client closes the connection.
};


int main() {
    Poco::Net::ServerSocket svs(2499); // Port number
    //Create the server
    //pass in a template of what to do with each new connection adn the socket to listen on
    Poco::Net::TCPServer srv(new Poco::Net::TCPServerConnectionFactoryImpl<JSONConnection>(), svs);

    srv.start(); //start listening
    ConnectToDB();
    std::cout << "Server is running" << std::endl;

    // Server runs until termination
    std::cin.get();
    srv.stop();
    return 0;
}

