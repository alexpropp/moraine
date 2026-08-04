#include "s3_secret.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace moraine_duckdb {

bool ResolveS3Config(duckdb::ClientContext &context, const std::string &path, MoraineS3Config &config,
                     S3SecretStrings &storage) {
	config = MoraineS3Config {};
	config.use_ssl = -1;
	if (!duckdb::StringUtil::StartsWith(path, "s3://")) {
		return false;
	}

	auto &secret_manager = duckdb::SecretManager::Get(context);
	auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, path, "s3");
	if (!match.HasMatch()) {
		return true;
	}

	auto &kv = dynamic_cast<const duckdb::KeyValueSecret &>(match.GetSecret());
	auto take = [&](const char *key, std::string &into, const char *&field) {
		duckdb::Value value;
		if (kv.TryGetValue(key, value) && !value.IsNull()) {
			into = value.ToString();
			field = into.c_str();
		}
	};
	take("key_id", storage.key_id, config.key_id);
	take("secret", storage.secret, config.secret);
	take("region", storage.region, config.region);
	take("session_token", storage.session_token, config.session_token);
	take("endpoint", storage.endpoint, config.endpoint);
	take("url_style", storage.url_style, config.url_style);
	duckdb::Value ssl;
	if (kv.TryGetValue("use_ssl", ssl) && !ssl.IsNull()) {
		config.use_ssl = ssl.GetValue<bool>() ? 1 : 0;
	}
	return true;
}

} // namespace moraine_duckdb
