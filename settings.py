# FTP site parameters
FTP_HOST = "pointconnect.commodities.thomsonreuters.com"
FTP_USER = "Data_Eneco"
FTP_PASS = "351EnD"

# FTP folder parameters
FTP_DIRECTORY = "/PCO_Eneco/PCO_Eneco/Live/Power/Supply"
#FTP_DIRECTORY = "/PCO_Eneco/PCO_Eneco/History/Power/Supply"
FILES_PREFIX = ["5034109_Pwr_PCA_PRO_Wind_ECOp_GBR_F_:Great Britain","5005795_Pwr_PCA_PRO_Wind_ECOP_NLD_F_:Netherlands","5024902_Pwr_PCA_PRO_Wind_ECop_BEL_F_:Belgium","5007707_Pwr_PCA_PRO_Wind_ECop_DEU_F_:Germany"]
#FILES_PREFIX = ["5034110_HIST_Pwr_PCA_PRO_Wind_ECOp_GBR_F_","5010585_HIST_Pwr_PCA_PRO_Wind_ECOP_NLD_F_","5024903_HIST_Pwr_PCA_PRO_Wind_ECop_BEL_F_","5010503_HIST_Pwr_PCA_PRO_Wind_ECop_DEU_F_"]


# Kafka Bootstrap server settings
BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "windforecast"

SLEEPER_TIME = 60