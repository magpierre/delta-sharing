
#ifndef __DELTASHARINGCLIENT_H__
#define __DELTASHARINGCLIENT_H__

#include <iostream>
#include "DeltaSharingRestClient.h"
#include <arrow/table.h>

#pragma once 
namespace DeltaSharing
{

    struct DeltaSharingClient 
    {
    public:
        DeltaSharingClient(std::string filename);
        std::shared_ptr<arrow::Table> ReadParquetFile(std::string &url);
        const std::shared_ptr<std::list<DeltaSharingProtocol::Share>> ListShares(int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::Schema>> ListSchemas(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> ListTables(const DeltaSharingProtocol::Schema &schema, int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> ListAllTables(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const;
        const std::shared_ptr<std::list<DeltaSharingProtocol::File>> ListFilesInTable(const DeltaSharingProtocol::Table) const;
        const DeltaSharingProtocol::Metadata QueryTableMetadata(const DeltaSharingProtocol::Table &table) const;
    protected:
    private:
        DeltaSharingRestClient restClient;
    };
};

#endif