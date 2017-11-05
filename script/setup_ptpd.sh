#!bin/bash
nodes_file="nodes3"

let "count=0"
for node in $(cat ${nodes_file}); do
  (echo "Setting up $node..."
  ssh -o "StrictHostKeyChecking no" $node "sudo apt-get install ptpd"
  ssh -o "StrictHostKeyChecking no" $node "sudo ptpd -i enp8s0d1 -s"
  ) &
  let "count += 1"
  if ! ((count % 8)); then
    wait
  fi
done
wait

echo "done."
