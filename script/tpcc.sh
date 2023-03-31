cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=0 -DNONTS=0 -DFAIR=0 -DRANDOM=0 ..
ninja
for i in 1 1 1 1 1 1 1 1
do 
    numactl --interleave=all ./tpcc_bamboo.exe -thread_num=224 -tpcc_num_wh=224
done

# cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=0 -DNONTS=1 -DFAIR=0 -DRANDOM=0 ..
# ninja
# for i in 56 112 168 224
# do 
#     numactl --interleave=all ./tpcc_bamboo.exe -thread_num=$i -tpcc_num_wh=$i
# done

# cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=0 -DNONTS=1 -DFAIR=1 -DRANDOM=0 ..
# ninja
# for i in  56 112 168 224
# do 
#     numactl --interleave=all ./tpcc_bamboo.exe -thread_num=$i -tpcc_num_wh=$i
# done

# cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBACK_OFF=0 -DNONTS=0 -DFAIR=0 -DRANDOM=1 ..
# ninja
# for i in  56 112 168 224
# do 
#     numactl --interleave=all ./tpcc_bamboo.exe -thread_num=$i -tpcc_num_wh=$i
# done