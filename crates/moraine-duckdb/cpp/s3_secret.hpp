// Resolving S3 credentials from the DuckDB secret that matches a store
// path. Shared by attach and by migrate, which reach the same stores by
// different routes.
#pragma once

#include "duckdb.hpp"

#include "moraine_abi.h"

#include <string>

namespace moraine_duckdb {

// Backing storage for the strings a `MoraineS3Config` points into. The ABI
// borrows them, so this must outlive the call that reads the config.
struct S3SecretStrings {
	std::string key_id;
	std::string secret;
	std::string region;
	std::string session_token;
	std::string endpoint;
	std::string url_style;
};

// Fills `config` from the DuckDB secret matching `path` — the same secret
// DuckLake and httpfs use for DATA_PATH — when `path` is an `s3://` URL.
// Fields the secret omits are left unset and fall back to the AWS_*
// environment in the core.
//
// Returns whether `path` is an `s3://` URL, which is what decides whether
// the caller passes `&config` or null.
bool ResolveS3Config(duckdb::ClientContext &context, const std::string &path, MoraineS3Config &config,
                     S3SecretStrings &storage);

} // namespace moraine_duckdb
