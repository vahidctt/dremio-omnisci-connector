# dremio-omnisci-connector
Dremio Omnisci Connector Plugin

Not fully tested.

<h2>Building and Installation</h2>
<ol>
<li>In root directory with the pom.xml file run <code>mvn clean install</code></li>
<li>Take the resulting .jar file in the target folder and put it in the \dremio\jars folder in Dremio</li>
<li>Take the Omnisci JDBC driver from your .m2 folder (or check https://github.com/omnisci/omnisci-jdbc) and put in in the \dremio\jars\3rdparty folder</li>
<li>Restart Dremio</li>
</ol>
