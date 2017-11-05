#!bin/bash
nodes_file="nodes"

let "count=0"
for node in $(cat ${nodes_file}); do
  (echo "Setting up $node..."
  ssh -o "StrictHostKeyChecking no" $node "echo 'options mlx4_core log_num_mtt=28' | sudo tee --append /etc/modprobe.d/mlx4.conf"
  ssh -o "StrictHostKeyChecking no" $node "echo 'options mlx4_core log_mtts_per_seg=4' | sudo tee --append /etc/modprobe.d/mlx4.conf"
  ssh -o "StrictHostKeyChecking no" $node "sudo modprobe -r iw_c2"
  ssh -o "StrictHostKeyChecking no" $node "sudo /etc/init.d/openibd restart"
  ssh -o "StrictHostKeyChecking no" $node "sudo modprobe iw_c2"
  ) &
  let "count += 1"
  if ! ((count % 8)); then
    wait
  fi
done
wait

echo "done."
