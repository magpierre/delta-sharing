

#include "include/DeltaSharingClient.h"
#include <parquet/stream_reader.h>
#include <arrow/io/memory.h>
#include <arrow/dataset/dataset.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <fstream>
#include <filesystem>
#include <exception>
#include <chrono>
#include <thread>

namespace DeltaSharing
{

    DeltaSharingClient::DeltaSharingClient(std::string filename, boost::optional<std::string> cacheLocation) : restClient(filename){
        auto path = std::filesystem::current_path().generic_string();
        std::cout << "Current path: " << path << std::endl;
        path.append("/cache");
        this->cacheLocation = cacheLocation.get_value_or(path);
         if(std::filesystem::exists(this->cacheLocation) == false)
            std::filesystem::create_directories(this->cacheLocation);

        if(std::filesystem::exists(this->cacheLocation) && std::filesystem::is_directory(this->cacheLocation)) {
            auto p = std::filesystem::status(this->cacheLocation).permissions();
            std::cout << "Cache directory:" << this->cacheLocation << " Permission: " << ((p & std::filesystem::perms::owner_read) != std::filesystem::perms::none ? "r" : "-")
              << ((p & std::filesystem::perms::owner_write) != std::filesystem::perms::none ? "w" : "-")
              << ((p & std::filesystem::perms::owner_exec) != std::filesystem::perms::none ? "x" : "-")
              << ((p & std::filesystem::perms::group_read) != std::filesystem::perms::none ? "r" : "-")
              << ((p & std::filesystem::perms::group_write) != std::filesystem::perms::none ? "w" : "-")
              << ((p & std::filesystem::perms::group_exec) != std::filesystem::perms::none ? "x" : "-")
              << ((p & std::filesystem::perms::others_read) != std::filesystem::perms::none ? "r" : "-")
              << ((p & std::filesystem::perms::others_write) != std::filesystem::perms::none ? "w" : "-")
              << ((p & std::filesystem::perms::others_exec) != std::filesystem::perms::none ? "x" : "-")
              << '\n';
        } 
       
    };

    std::shared_ptr<arrow::Table> DeltaSharingClient::ReadParquetFile(std::string &url)
    {
        if (url.length() == 0)
            return std::shared_ptr<arrow::Table>();

        auto r = this->restClient.get(url);
        int cnt = 0;
        std::cout << url << " code: " << r.code << std::endl;
        while (this->restClient.shouldRetry(r))
        {
            cnt++;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (cnt > 4)
            {
                std::cout << "Failed to retrieve file using url: ( Response code: " << r.code << ") Message: " << r.body << std::endl;
                return std::shared_ptr<arrow::Table>();
            }
            r = this->restClient.get(url);
        }

        if (r.code != 200) {
            std::cout << "Could not read file: " << r.code << " Message: " << r.body <<  std::endl;
            return std::shared_ptr<arrow::Table>();
        }
        auto pos = url.find_first_of('?', 8);
        auto path = url.substr(8, pos - 8); // Removing "https://"

        std::vector<std::string> urlparts;
        while ((pos = path.find("/")) != std::string::npos)
        {
            urlparts.push_back(path.substr(0, pos));
            path.erase(0, pos + 1);
        }
        if (urlparts.size() != 3)
        {
            std::cout << "Invalid URL:" << url << std::endl;
            return std::shared_ptr<arrow::Table>();
        }
        std::string tbl = urlparts.back();
        urlparts.pop_back();
        std::string schema = urlparts.back();
        urlparts.pop_back();
        std::string share = urlparts.back();

        std::cout << "Share: " << share << std::endl
                  << "Schema: " << schema << std::endl
                  << "Table: " << tbl << std::endl
                  << "File: " << path << std::endl;

        auto completePath = this->cacheLocation + "/" + share + "/" + schema + "/" + tbl;

        std::fstream f;
        std::filesystem::create_directories(completePath);
        f.open(completePath + "/" + path, std::ios::trunc | std::ios::out | std::ios::binary);
        f.write(r.body.c_str(), r.body.size());
        f.flush();
        f.close();
        std::shared_ptr<arrow::io::ReadableFile> infile;
        try
        {
            PARQUET_ASSIGN_OR_THROW(infile,
                                    arrow::io::ReadableFile::Open(completePath + "/" + path));
        }
        catch (parquet::ParquetStatusException e)
        {
            std::cout << "error code:(" << e.status() << ") Message: " << e.what() << std::endl;
            return std::shared_ptr<arrow::Table>();
        }
        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

        return table;
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Share>> DeltaSharingClient::ListShares(int maxResult, std::string pageToken) const
    {
        return this->restClient.ListShares(maxResult, pageToken);
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Schema>> DeltaSharingClient::ListSchemas(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const
    {
        return this->restClient.ListSchemas(share, maxResult, pageToken);
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> DeltaSharingClient::ListTables(const DeltaSharingProtocol::Schema &schema, int maxResult, std::string pageToken) const
    {
        return this->restClient.ListTables(schema, maxResult, pageToken);
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::Table>> DeltaSharingClient::ListAllTables(const DeltaSharingProtocol::Share &share, int maxResult, std::string pageToken) const
    {
        return this->restClient.ListAllTables(share, maxResult, pageToken);
    };

    const std::shared_ptr<std::list<DeltaSharingProtocol::File>> DeltaSharingClient::ListFilesInTable(const DeltaSharingProtocol::Table table) const
    {
        return this->restClient.ListFilesInTable(table);
    };

     const DeltaSharingProtocol::Metadata DeltaSharingClient::QueryTableMetadata(const DeltaSharingProtocol::Table &table) const {
        return this->restClient.QueryTableMetadata(table);
     };
};