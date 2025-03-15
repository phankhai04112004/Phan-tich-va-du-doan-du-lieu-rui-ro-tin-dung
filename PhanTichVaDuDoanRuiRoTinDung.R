# Cài đặt và tải các thư viện cần thiết
install.packages(c("sparklyr", "dplyr", "ggplot2", "readr", "readxl"))

library(sparklyr)  # Thư viện kết nối và làm việc với Spark
library(dplyr)     # Thư viện xử lý dữ liệu
library(ggplot2)   # Thư viện vẽ biểu đồ
library(readr)     # Đọc tệp CSV
library(readxl)    # Đọc tệp Excel

# Kết nối với Spark
sc <- spark_connect(master = "local")

# Đọc dữ liệu từ các tệp
df <- read.csv("C:/Users/Admin/Downloads/BigData/loan/loan.csv")  # Đọc tệp CSV
dic <- read_excel("C:/Users/Admin/Downloads/BigData/LCDataDictionary.xlsx")  # Đọc tệp Excel

# Kiểm tra thông tin về dữ liệu
str(df)  # Cấu trúc của dữ liệu loan.csv
str(dic) # Cấu trúc của dữ liệu LCDataDictionary.xlsx
dim(df)  # Kích thước dữ liệu

# Kiểm tra các cột trong dữ liệu
columns <- colnames(df)
print(columns)

# Tính tỷ lệ phần trăm các giá trị thiếu trong mỗi cột
null_percentages <- sapply(df, function(x) mean(is.na(x)) * 100)
for (column in names(null_percentages)) {
  column_percentage <- round(null_percentages[column], 5)
  column_type <- class(df[[column]])
  print(paste(column, ": ", column_percentage, "%, Type:", column_type))
}

# Lấy dòng đầu tiên và chuyển nó thành một vector
first_row <- df[1, ]
first_row_vector <- as.vector(first_row)
print(first_row_vector[1:50])  # In ra 50 giá trị đầu tiên

# Kiểm tra số lượng giá trị duy nhất trong mỗi cột
for (i in colnames(df)) {
  print(paste(i, ":", length(unique(df[[i]]))))
}

# Kiểm tra các giá trị duy nhất trong loan_status
unique(df$loan_status)

# Tính toán số lượng các giá trị trong loan_status và vẽ biểu đồ cột
status_counts <- table(df$loan_status)
ggplot(as.data.frame(status_counts), aes(x = Var1, y = Freq)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  labs(title = "Distribution of Loan Status", x = "Loan Status", y = "Count") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Chọn các cột cần thiết để phân tích
df_selected <- df[, c('loan_amnt', 'int_rate', 'term', 'dti', 'annual_inc', 'delinq_2yrs', 'open_acc', 'grade', 
                      'home_ownership', 'collections_12_mths_ex_med', 'revol_bal', 'total_acc', 'loan_status')]

# Kiểm tra số lượng giá trị thiếu trong các cột
colSums(is.na(df_selected))
df_selected <- na.omit(df_selected)  # Loại bỏ các dòng có giá trị NA
colSums(is.na(df_selected))
dim(df_selected)
head(df_selected, 10)

# Chuyển đổi loan_status thành các nhóm
df_selected <- df_selected %>%
  mutate(loan_status = case_when(
    loan_status %in% c("Fully Paid", "In Grace Period", "Issued") ~ "Normal",
    loan_status %in% c("Late (16-30 days)", "Late (31-120 days)") ~ "Delinquent",
    loan_status %in% c("Charged Off", "Default") ~ "Default",
    grepl("Does not meet the credit policy", loan_status) ~ "Not Compliant",
    grepl("Current", loan_status) ~ "Current",
    TRUE ~ "Unknown"
  ))

# Kiểm tra các giá trị duy nhất trong loan_status sau khi phân loại
unique(df_selected$loan_status)

# Vẽ biểu đồ violin cho phân phối loan amount theo loan status
ggplot(df_selected, aes(x = loan_status, y = loan_amnt)) +
  geom_violin() +
  labs(title = "Loan Amount Distribution by Loan Status",
       x = "Loan Status", y = "Loan Amount") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Chuyển đổi grade thành factor để đảm bảo thứ tự hiển thị đúng
df_selected$grade <- factor(df_selected$grade, levels = c('A', 'B', 'C', 'D', 'E', 'F', 'G'))

# Vẽ biểu đồ boxplot cho phân phối loan amount theo grade
ggplot(df_selected, aes(x = grade, y = loan_amnt)) +
  geom_boxplot(fill = "lightblue") +
  labs(title = "Loan Amount Distribution by Grade", x = "Grade", y = "Loan Amount") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Vẽ biểu đồ countplot cho loan status phân theo grade
ggplot(df_selected, aes(x = grade, fill = loan_status)) +
  geom_bar(position = "dodge") +
  scale_fill_brewer(palette = "Set2") +
  labs(title = "Loan Status Distribution by Grade", x = "Grade", y = "Count", fill = "Loan Status") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Vẽ biểu đồ boxplot cho lãi suất (interest rate) theo loan status
ggplot(df_selected, aes(x = loan_status, y = int_rate)) +
  geom_boxplot(fill = "skyblue", color = "darkblue") +
  labs(title = "Interest Rate by Loan Status", x = "Loan Status", y = "Interest Rate") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Vẽ biểu đồ boxplot cho thu nhập hàng năm theo loan status và sử dụng log scale cho trục y
ggplot(df_selected, aes(x = loan_status, y = annual_inc)) +
  geom_boxplot(fill = "skyblue", color = "darkblue") +
  scale_y_continuous(trans = 'log10') +
  labs(title = "Annual Income by Loan Status (Log Scale)",
       x = "Loan Status", y = "Annual Income (Log Scale)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Tạo bảng chéo để tính tỷ lệ phần trăm của loan status theo home ownership
cross_tab <- table(df_selected$home_ownership, df_selected$loan_status)
cross_tab_percentage <- prop.table(cross_tab, 1)
cross_tab_percentage_df <- as.data.frame(cross_tab_percentage)
colnames(cross_tab_percentage_df) <- c("Home_Ownership", "Loan_Status", "Proportion")

# Vẽ biểu đồ thanh chồng cho tỷ lệ loan status theo home ownership
ggplot(cross_tab_percentage_df, aes(x = Home_Ownership, y = Proportion, fill = Loan_Status)) +
  geom_bar(stat = "identity", position = "stack") +
  labs(title = "Proportion of Loan Status by Home Ownership",
       x = "Home Ownership", y = "Proportion") +
  scale_fill_brewer(palette = "Blues") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  theme(legend.title = element_text(text = "Loan Status")) +
  theme(legend.position = "top") +
  theme(legend.box = "horizontal")

# Định nghĩa hàm mã hóa nhãn
SC_LabelEncoder1 <- function(text) {
  if (text == "A") {
    return(1)
  } else if (text == "B") {
    return(2)
  } else if (text == "C") {
    return(3)
  } else if (text == "D") {
    return(4)
  } else if (text == "E") {
    return(5)
  } else if (text == "F") {
    return(6)
  } else if (text == "G") {
    return(7)
  }
}

SC_LabelEncoder2 <- function(text) {
  if (text == " 36 months") {
    return(1)
  } else {
    return(2)
  }
}

SC_LabelEncoder3 <- function(text) {
  if (text == "RENT") {
    return(1)
  } else if (text == "MORTGAGE") {
    return(2)
  } else if (text == "OWN") {
    return(3)
  } else {
    return(0)
  }
}

# Áp dụng các hàm mã hóa
df_selected$grade <- sapply(df_selected$grade, SC_LabelEncoder1)
df_selected$term <- sapply(df_selected$term, SC_LabelEncoder2)
df_selected$home_ownership <- sapply(df_selected$home_ownership, SC_LabelEncoder3)

# Kiểm tra các giá trị duy nhất trong loan_status
as.data.frame(table(df_selected$loan_status))

# Tạo biểu đồ phân phối loan status
loan_status_counts <- as.data.frame(table(df_selected$loan_status))
colnames(loan_status_counts) <- c("Loan Status", "Count")
ggplot(loan_status_counts, aes(x = Count, y = `Loan Status`)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  theme_minimal() +
  ggtitle("Distribution of Loan Status") +
  xlab("Count") +
  ylab("Loan Status")

# Mã hóa loan_status thành các giá trị số
df_selected$loan_status_encoded <- ifelse(df_selected$loan_status == "Normal", 0,
                                          ifelse(df_selected$loan_status %in% c("Default", "Delinquent", "Not Compliant"), 1, 2))

# Kiểm tra tần suất xuất hiện của loan_status_encoded
table(df_selected$loan_status_encoded)

# Vẽ biểu đồ phân phối loan_status_encoded
loan_status_counts <- as.data.frame(table(df_selected$loan_status_encoded))
colnames(loan_status_counts) <- c('Loan Status', 'Count')
ggplot(loan_status_counts, aes(x = `Loan Status`, y = Count)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(title = "Distribution of Loan Status", x = "Loan Status", y = "Count") +
  theme_minimal()

# Loại bỏ các dòng có loan_status là 'Current'
df_selected <- df_selected[df_selected$loan_status != "Current", ]

# Kiểm tra lại loan_status sau khi loại bỏ 'Current'
unique(df_selected$loan_status)

# Ngắt kết nối với Spark
spark_disconnect(sc)
