// WAY I DO IT IS
//1. The topics are formed and topic has (total number of validator nodes/number of topics)
//2. Then there are peer connections formed between the nodes in the topic such that a node has 8 direct peers(local mesh)

//3. The traffic generator intially calls the block proposer

//4. Block proposer distributes it 3 ways

//a. Send half numbers of 1 row/col(as the reciever node can reconstruct the whole row/col)
// After the node recieves it, it gossip a IHAVE message with the metadata to all the nodes in the topic about the data is recieved from the BP
// The recieving node randomly sends IWANT message to one node for recieving the whole row/col
// Then start sampling 75 times
// If the sample request is timed out it will send it again

// METHOD-2
//b. The block propsoser divides the rows/cols into 8 parts
// The first 8 nodes in the topic will recieve 8 parts, like 1st node will recieve 1st part of all the 8 rows and so on.
// It will gossip the IHAVE message to all the topic nodes but over no node will send the IWANT message
// Then the node will send 3 requests to get another 3 different parts.
// If it recieves those parts it can reconstruct the whole row/col as it will have the 50% of the cells.
// Timeout mechanism is applied to these messages as well
// After recieving the requested parts, it starts sampling

// METHOD-3
//c. The block proposer will send  half of each row/col to 2 nodes.
// Like the first 2 nodes hold row 1 and row 2 and so on.
// The nodes will send IHAVE metadata messages to all the topic members but they wont send IWANT in return as it each node has 2 rows/cols
// And then start sampling


//Changes made on 27-01-2025
//1. In congfig file
//2.

// Update to the simulation on 27-01-2025
//1. Made number of validators nodes equal to 8192
//2. Number of topic is same(=64). Since there are 64 committees per slot and there are 32 slots in a epoch. And since the committees are decided at the start of the epoch,
// we can assign the topics based on it. So like topic 0 will have all the validator nodes from all the 32 slots who are the members of committee 1 and so....
//3. Each topic will hold 8 rows and 8 cols.
//4. Each row/col will be held by 8 validator nodes
//5. Each topic will have 128 nodes.

// Method -2 distribution implementation
// Each topic will have 128 nodes and will manage 8 rows and 8 cols
// First 64 nodes will be responsible for rows whereas the next 64 nodes will be responsible for cols
// All the rows and cols will be divided into 8 parts
// The first 8 nodes in topic will hold first part of all 8 rows, next 8 will hold 2nd parts of all rows and so on.....