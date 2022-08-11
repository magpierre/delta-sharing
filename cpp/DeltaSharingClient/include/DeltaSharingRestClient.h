
#ifndef __DELTASHARINGRESTCLIENT_H__
#define __DELTASHARINGRESTCLIENT_H__

#include <iostream>
#include <nlohmann/json.hpp>
#include <list>
#include "Protocol.h"
#include <restclient-cpp/restclient.h>
#include <restclient-cpp/connection.h>

using json = nlohmann::json;

namespace DeltaSharing
{
    struct DeltaSharingRestClient
    {
    public:
        DeltaSharingRestClient(std::string filename);
        ~DeltaSharingRestClient();
        const std::shared_ptr<std::list<DeltaSharingProtocol::Share>> ListShares(int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::Schema>> ListSchemas(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> ListTables(const DeltaSharingProtocol::Schema &schema, int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> ListAllTables(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::File>> ListFilesInTable(const DeltaSharingProtocol::Table) const;
        const DeltaSharingProtocol::Metadata QueryTableMetadata(const DeltaSharingProtocol::Table &table) const;
        const DeltaSharingProtocol::DeltaSharingProfile &GetProfile() const;
        const RestClient::Response get(std::string url) const;
        const bool shouldRetry(RestClient::Response &response) const;

    protected:
        json ReadFromFile(std::string filename);

    private:
        DeltaSharingProtocol::DeltaSharingProfile profile;
    };
};

#endif