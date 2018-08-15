data_master  <- read.csv("/Users/hgies/Projects/tremor-runtime/bench/results/old.txt", header = FALSE)
data_master <- lapply(data_master, diff)

data_all_keys  <- read.csv("/Users/hgies/Projects/tremor-runtime/bench/results/all-keys.txt", header = FALSE)
data_all_keys <- lapply(data_all_keys, diff)

data_hashmap  <- read.csv("/Users/hgies/Projects/tremor-runtime/bench/results/hashmap.txt", header = FALSE)
data_hashmap <- lapply(data_hashmap, diff)

data_array  <- read.csv("/Users/hgies/Projects/tremor-runtime/bench/results/array.txt", header = FALSE)
data_array <- lapply(data_array, diff)

data_json_old <- read.csv("/Users/hgies/Projects/tremor-runtime/bench/results/json-old.txt", header = FALSE)
data_json_old <- lapply(data_json_old, diff)

data_json  <- read.csv("/Users/hgies/Projects/tremor-runtime/bench/results/mimir-json.txt", header = FALSE)
data_json <- lapply(data_json, diff)

df <- data.frame(master = data_master$V1, all_keys = data_all_keys$V1, hashmap = data_hashmap$V1, array = data_array$V1, json_old=data_json_old$V1, json=data_json$V1)
boxplot(df)
