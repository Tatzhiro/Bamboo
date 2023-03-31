wh=1

cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=1 -DNONTS=0 -DFAIR=0 -DRANDOM=0 ..
ninja

for i in 8 16 32 64 96 120
do 
    numactl --interleave=all ./tpcc_bamboo.exe -thread_num=$i -tpcc_num_wh=$wh
done

cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=1 -DNONTS=1 -DFAIR=0 -DRANDOM=0 ..
ninja
for i in 8 16 32 64 96 120
do 
    numactl --interleave=all ./tpcc_bamboo.exe -thread_num=$i -tpcc_num_wh=$wh
done

cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=1 -DNONTS=1 -DFAIR=1 -DRANDOM=0 ..
ninja
for i in 8 16 32 64 96 120
do 
    numactl --interleave=all ./tpcc_bamboo.exe -thread_num=$i -tpcc_num_wh=$wh
done

cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=1 -DNONTS=0 -DFAIR=0 -DRANDOM=1 ..
ninja
for i in 8 16 32 64 96 120
do 
    numactl --interleave=all ./tpcc_bamboo.exe -thread_num=$i -tpcc_num_wh=$wh
done