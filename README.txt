---------------------------
Affinity Propogation on Hadoop Cluster.
---------------------------


This is a java-distributive implementation of Affinity Propagation.
Any questions or comments about this code should be sent to songxiaoli2012@gmail.com.

------------------------------------------------------------------------

TABLE OF CONTENTS

A. PREREQUISITE.

B. DATA INPUT FORMAT.

C. RESULT.

------------------------------------------------------------------------

A. PREREQUISITE.

The Hadoop cluster needs to be installed before running the jar files.

------------------------------------------------------------------------

B. DATA INPUT FORMAT.

The data is a file where each line is of the form:

[M] [term_1] [term_2] ... [term_N]

* [M] is the document id.

* [term_i] is the i-th term in document M.

------------------------------------------------------------------------

C. RESULT.
The result is in the folder <working directory>/Cluster. It gives each document (given as document id) to a cluster (represented by the cluster center given as document id.)
