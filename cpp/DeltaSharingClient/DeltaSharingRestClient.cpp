
#include "include/DeltaSharingRestClient.h"
#include <fstream>
#include <restclient-cpp/restclient.h>
#include <restclient-cpp/connection.h>

namespace DeltaSharing
{

    DeltaSharingRestClient::DeltaSharingRestClient(std::string filename)
    {
        json j = ReadFromFile(filename);
        this->profile = j;
        RestClient::init();
    };

    DeltaSharingRestClient::~DeltaSharingRestClient()
    {
        std::cout << "DeltaSharingRestClient destructed" << std::endl;
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Share>> DeltaSharingRestClient::ListShares(int maxResult, std::string pageToken) const
    {
        std::unique_ptr<RestClient::Connection> c = std::unique_ptr<RestClient::Connection>(new RestClient::Connection(this->profile.endpoint));
        RestClient::Response r = c->get("/shares");
        json j = json::parse(r.body);
        auto items = j["items"];
        std::shared_ptr<std::list<DeltaSharingProtocol::Share>> p;
        p = std::make_shared<std::list<DeltaSharingProtocol::Share>>();
        for (auto it = items.begin(); it < items.end(); it++)
        {
            DeltaSharingProtocol::Share s = it.value();
            p->push_back(s);
        }
        return p;
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Schema>> DeltaSharingRestClient::ListSchemas(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const
    {
        std::unique_ptr<RestClient::Connection> c = std::unique_ptr<RestClient::Connection>(new RestClient::Connection(this->profile.endpoint));
        std::string path = "/shares/" + share.name + "/schemas";
        RestClient::Response r = c->get(path);
        json j = json::parse(r.body);
        auto items = j["items"];
        std::shared_ptr<std::list<DeltaSharingProtocol::Schema>> p;
        p = std::make_shared<std::list<DeltaSharingProtocol::Schema>>();
        for (auto it = items.begin(); it < items.end(); it++)
        {
            DeltaSharingProtocol::Schema s = it.value().get<DeltaSharingProtocol::Schema>();
            p->push_back(s);
        }
        return p;
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> DeltaSharingRestClient::ListTables(const DeltaSharingProtocol::Schema &schema, int maxResult, std::string pageToken) const
    {
        std::unique_ptr<RestClient::Connection> c = std::unique_ptr<RestClient::Connection>(new RestClient::Connection(this->profile.endpoint));
        std::string path = "/shares/" + schema.share + "/schemas/" + schema.name + "/tables";
        RestClient::Response r = c->get(path);
        json j = json::parse(r.body);
        auto items = j["items"];
        std::shared_ptr<std::list<DeltaSharingProtocol::Table>> t;
        t = std::make_shared<std::list<DeltaSharingProtocol::Table>>();
        for (auto it = items.begin(); it < items.end(); it++)
        {
            DeltaSharingProtocol::Table s = it.value();
            t->push_back(s);
        }
        return t;
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> DeltaSharingRestClient::ListAllTables(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const
    {
        std::unique_ptr<RestClient::Connection> c = std::unique_ptr<RestClient::Connection>(new RestClient::Connection(this->profile.endpoint));
        std::string path = "/shares/" + share.name + "/all-tables";
        RestClient::Response r = c->get(path);
        json j = json::parse(r.body);
        auto items = j["items"];
        std::shared_ptr<std::list<DeltaSharingProtocol::Table>> t;
        t = std::make_shared<std::list<DeltaSharingProtocol::Table>>();
        for (auto it = items.begin(); it < items.end(); it++)
        {
            DeltaSharingProtocol::Table s = it.value();
            t->push_back(s);
        }
        return t;
        return NULL;
    };

    const DeltaSharingProtocol::Metadata DeltaSharingRestClient::QueryTableMetadata(const DeltaSharingProtocol::Table &table) const {
        std::unique_ptr<RestClient::Connection> c = std::unique_ptr<RestClient::Connection>(new RestClient::Connection(this->profile.endpoint));
        std::string path = "/shares/" + table.share + "/schemas/" + table.schema + "/tables/" + table.name + "/metadata";
        RestClient::Response r = c->get(path);
        std::istringstream input;
        input.str(r.body);
        json j;
        DeltaSharingProtocol::Metadata m;
        int cnt = 0;
        for (std::string line; std::getline(input, line); cnt++)
        {
            if (cnt == 1)
            {
                j = json::parse(line);
                m = j["metaData"];
            }
        }

        return m;
    };

    json DeltaSharingRestClient::ReadFromFile(std::string filename)
    {
        std::ifstream is;
        is.open(filename, std::ifstream::in);
        json j;
        is >> j;
        is.close();
        return j;
    };

    const DeltaSharingProtocol::DeltaSharingProfile &DeltaSharingRestClient::GetProfile() const
    {
        return this->profile;
    }

    const std::shared_ptr<std::list<DeltaSharingProtocol::File>> DeltaSharingRestClient::ListFilesInTable(DeltaSharingProtocol::Table table) const
    {
        std::unique_ptr<RestClient::Connection> c = std::unique_ptr<RestClient::Connection>(new RestClient::Connection(this->profile.endpoint));
        std::string path = "/shares/" + table.share + "/schemas/" + table.schema + "/tables/" + table.name + "/query";
        RestClient::HeaderFields h;
        h.insert({"Content-Type", "application/json; charset=UTF-8"});
        h.insert({"Authorization", "Bearer: " + this->profile.bearerToken});
        c->SetHeaders(h);
        DeltaSharingProtocol::data d;
        json j = d;
        RestClient::Response r = c->post(path, j.dump());
        int cnt = 0;
        std::istringstream input;
        input.str(r.body);
        std::shared_ptr<std::list<DeltaSharingProtocol::File>> t;
        t = std::make_shared<std::list<DeltaSharingProtocol::File>>();
        for (std::string line; std::getline(input, line); cnt++)
        {
            if (cnt > 1)
            {
                json jf = json::parse(line);
                json jf2 = jf["file"];
                DeltaSharingProtocol::File f = jf2;
                t->push_back(f);
            }
        }

        return t;
    };
    const RestClient::Response DeltaSharingRestClient::get(std::string url) const
    {
        std::unique_ptr<RestClient::Connection> c = std::unique_ptr<RestClient::Connection>(new RestClient::Connection(this->profile.endpoint));
        RestClient::Response r = c->get(url);
        return r;
    };


    const bool DeltaSharingRestClient::shouldRetry(RestClient::Response &r) const {
    if(r.code == 200)
        return false;
	if (r.code == 429) {
		std::cout << "Retry operation due to status code: 429" << std::endl;
		return true;
	} else if (r.code >= 500 && r.code < 600 ) {
		std::cout << "Retry operation due to status code: " << r.code << std::endl;
		return true;
	} else 
		return false;
	
    };

};