 #!/bin/bash

clear
echo "=== clean - compile - package ==="
mvn clean compile package
echo "=== deploy on storm ==="
cd target/
storm jar ViewToStream-1.0-SNAPSHOT.jar ViewUpdateTopology
echo "=== deploy finished	 ==="