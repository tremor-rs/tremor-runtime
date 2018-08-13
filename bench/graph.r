data_master  <- read.csv("/Users/hgies/Projects/tremor-runtime/result1.txt.master", header = FALSE)
data_master <- lapply(data_master, diff)

data_all_keys  <- read.csv("/Users/hgies/Projects/tremor-runtime/result1.txt.all-keys", header = FALSE)
data_all_keys <- lapply(data_all_keys, diff)

data_hashmap  <- read.csv("/Users/hgies/Projects/tremor-runtime/result1.txt.hashmap", header = FALSE)
data_hashmap <- lapply(data_hashmap, diff)

data_array  <- read.csv("/Users/hgies/Projects/tremor-runtime/result1.txt.array", header = FALSE)
data_array <- lapply(data_array, diff)



df <- data.frame(master = data_master$V1, all_keys = data_all_keys$V1, hashmap = data_hashmap$V1, array = data_array$V1)
boxplot(df)
