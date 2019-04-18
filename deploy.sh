#!/bin/bash
echo "Copying jar to sandbox"
scp -P 2222 -i ~/.ssh/id_rsa_hdp target/scala-2.11/mayank_k_rastogi_hw5-assembly-0.1.jar root@sandbox-hdp.hortonworks.com:~/
echo "Logging in to sandbox as root"
ssh  -p 2222 -i ~/.ssh/id_rsa_hdp root@sandbox-hdp.hortonworks.com /bin/bash <<'ENDSSH'
echo "Checking if output_dir exists already"
hdfs dfs -test -d output_dir
if [ $? == 0 ]
then
    echo "output_dir already exists! Deleting the directory..."
    hdfs dfs -rm -r -skipTrash output_dir
    echo "output_dir removed"
else
    echo "output_dir does not exist"
fi
echo "Starting spark job..."
spark-submit --deploy-mode cluster mayank_k_rastogi_hw5-assembly-0.1.jar input_dir_full/dblp.xml output_page_rank
ENDSSH