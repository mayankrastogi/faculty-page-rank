## CS 441 - Engineering Distributed Objects for Cloud Computing
## Homework 5 - Faculty Page Rank

---

### Overview

The objective of this homework was to process the [DBLP](https://dblp.uni-trier.de/) dataset using **Apache Spark framework** to find out the **Page Rank** of publications made by faculty in the Department of Computer Science at the University of Illinois at Chicago.

### Instructions

#### My Environment

The project was developed using the following environment:

- **OS:** Windows 10
- **IDE:** IntelliJ IDEA Ultimate 2018.3
- **Hypervisor:** VMware Workstation 15 Pro
- **Hadoop Distribution:** [Hortonworks Data Platform (3.0.1) Sandbox](https://hortonworks.com/products/sandbox/) deployed on VMware

#### Prerequisites

- [HDP Sandbox](https://hortonworks.com/products/sandbox/) set up and deployed on (VMware or VirtualBox). Read this [guide](https://hortonworks.com/tutorial/learning-the-ropes-of-the-hortonworks-sandbox/) for instructions on how to set up and use HDP Sandbox
- Ability to use SSH and SCP on your system
- [SBT](https://www.scala-sbt.org/) installed on your system
- [dblp.xml](https://dblp.uni-trier.de/xml/) downloaded on your system

#### Running the map reduce job

1. Clone or download this repository onto your system
2. Open the Command Prompt (if using Windows) or the Terminal (if using Linux/Mac) and browse to the project directory
3. Build the project and generate the jar file using SBT
    
    ```
    sbt clean compile assembly
    ```
    
4. Start HDP sandbox VM
5. Copy the jar file  to HDP Sandbox VM
    
    ```
    scp -P 2222 target/scala-2.11/mayank_k_rastogi_hw5-assembly-0.1.jar root@sandbox-hdp.hortonworks.com:~/
    ```
    
6. Copy `dblp.xml` to HDP Sandbox
    
    ```
    scp -P 2222 /path/to/dblp.xml root@sandbox-hdp.hortonworks.com:~/
    ```
    
7. SSH into HDP Sandbox
    
    ```
    ssh -p 2222 root@sandbox-hdp.hortonworks.com
    ```
    
8. Create input directory on HDFS and copy `dblp.xml` there
    
    ```
    hdfs dfs -mkdir input_dir
    
    hdfs dfs -put dblp.xml input_dir/
    ```
    
9. Submit the Spark Application and run it in `cluster` mode
    
    ```
    spark-submit --deploy-mode cluster mayank_k_rastogi_hw5-assembly-0.1.jar input_dir/dblp.xml output_dir
    ```
    
    If you wish to run it in `local` mode instead, you will have to pass a parameter to the `jar` file specifying the number of iterations to run the page rank algorithm for, along with a parameter `"local"`
    
    ```
    spark-submit mayank_k_rastogi_hw5-assembly-0.1.jar input_dir/dblp.xml output_dir 100 local
    ```
    
10. Once the application is finished running, copy the output files from `output_dir` (on HDFS) to `page_rank_output` (on HDP Sandbox)
    
    ```
    hdfs dfs -get output_dir page_rank_output
    ```
    
11. Inside the `page_rank_output` directory, you will find the page ranks of UIC CS faculty under the `ranks_faculty` directory and the page ranks for their publications under the `ranks_venues` directory
 
### Tutorial on how to deploy the the Spark application on AWS EMR

To see a demo of how this project can be deployed on AWS Elastic Map Reduce (EMR), see this [video on YouTube](https://youtu.be/Ako_EXuIyPs)

[![How to deploy Apache Spark application on AWS Elastic Map Reduce (EMR)](https://img.youtube.com/vi/Ako_EXuIyPs/maxresdefault.jpg)](https://youtu.be/Ako_EXuIyPs)

### Working of the Spark Application

#### The `dblp.xml` file

The `dblp.xml` file has `<dblp>` as the root element. Under `<dblp>`, we can have `<article>`, `<inproceedings>`, `<proceedings>`, `<book>`, `<incollection>`, `<phdthesis>`, `<mastersthesis>` or `<www>`. Since `<www>` holds information about an author and not a publication itself, we are ignoring this tag in our program.

Except `<book>` and `<proceedings>`, each of these tags contain one or more `<author>` tags which denote an author for that publication. The `<book>` and `<proceedings>` tags have a similar tag `<editor>`. We treat both these tags as the same, which means that the presence of a faculty's name in either of these tags will count towards their total number of publications and collaborations.

The publication venue can be denoted by any of `<journal>`, `<publisher>`, `<school>`, or `<booktitle>` tags. The first match of any of these tags in a publication is designated as its publication venue.

#### Sharding the input file into logical splits

The `MultiTagXmlInputFormat` takes care of sharding the `dblp.xml` file into logical subsets that are fed into the `DBLPPageRank.processPublications` method. It reads the `dblp.xml` file and looks for one of these start tags - `<article `, `<inproceedings `, `<proceedings `, `<book `, `<incollection `, `<phdthesis `, `<mastersthesis `. Once a match is found, it stores all the bytes that appear after the matched start tag, into a buffer, until the corresponding end tag is found. This forms our logical split. The start and end tags to look for can be configured using `dblp-page-rank.xml-input.start-tags` and `dblp-page-rank.xml-input.end-tags` configuration settings in `application.conf`.

#### Extracting the authors and the publication venue from a publication

Each XML string, denoting a single publication from the `dblp.xml` file, is matched with two **regular expressions** to extract information about its authors and the publication venue. The authors of the publication are extracted using the below regex pattern, where the second capturing group contains the name of the author:

```regexp
\<(author|editor).*>(.*)\<\/(author|editor)>
```

Likewise, the publication venue is extracted using the below regex pattern, where the second capturing group contains the name of the publication venue:

```regexp
\<\b(journal|publisher|school|booktitle)\b>(.*)\<\/(journal|publisher|school|booktitle)>
```

Regular Expressions were used instead of an XML parser because of the performance overhead of XML parsers. The XML in each publication of the `dblp.xml` file contains entities that are defined in the `dblp.dtd` file. XML parsers fail to parse this XML without the DTD file. If we include the DTD file, for each publication, the XML parser will need to construct its grammar using the DTD file before it is able to parse the publication, which is a very expensive process. In the [map-reduce homework](https://github.com/mayankrastogi/faculty-collaboration), we were able to cache the instance of our XML parser, by defining it as a member variable of our `FacultyCollaborationMapper` class, which allowed reuse of the grammar constructed from the DTD file. However, **XML parser is not thread-safe** and does not allow parsing of more than one XML documents at once. Since Spark, spawns multiple threads to process the input in parallel, we need to create a new instance of XML parser for every publication that we need to parse, making this process very slow.

> For comparison, when my Spark application was using *XML parser* for this purpose, it took over **2 hours** to process the full **2.5 GB** `dblp.xml` file. In contrast, it takes just **4 minutes** to do the same using *Regular Expressions*!

The `DBLPPageRank.extractAuthorsAndVenueFromPublication` method extracts the authors and venues from a `String` representing a single publication from the `dblp.xml` file and matches them against the list of faculty that belong to UIC's CS department, which is defined in `src/main/resources/uic-cs-faculty-list.txt`. This file maps the different variations of a faculty's name (that are known to appear in the `dblp.xml` file) to the faculty's name as it appears on the [UIC CS Department Website](https://cs.uic.edu/faculty/?).

Each author is linked to every other author from this publication along with the publication venue, while the publication venue does not have any outgoing links. If either authors or publication venue could not be extracted, an empty sequence is returned.

Consider the following publication in `dblp.xml`:

```xml
<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
    <author>Mark Grechanik</author>
    <author>B. M. Mainul Hossain</author>
    <author>Ugo Buy</author>
    <title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
    <pages>174-183</pages>
    <year>2013</year>
    <booktitle>ICST</booktitle>
    <ee>https://doi.org/10.1109/ICST.2013.19</ee>
    <ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
    <crossref>conf/icst/2013</crossref>
    <url>db/conf/icst/icst2013.html#GrechanikHB13</url>
</inproceedings>
```

The list of authors from this publication will be `Seq("Mark Grechanik", "Ugo Buy")` and the publication venue will be `"ICST"`. Hence, the output of this method will be:

```scala
Seq(
  ("Mark Grechanik", Set("Ugo Buy", "ICST")),
  ("Ugo Buy", Set("Mark Grechanik", "ICST")),
  ("ICST", Set())
)
```

The `DBLPPageRank.processPublication` method will then flatten this list and reduce the set of edges for each author/venue by merging all the sets of edges belonging to the same author/venue.

#### Computing the Page Rank

Once we obtain our RDD from the `DBLPPageRank.processPublication` method in the format described above, we use the `DBLPPageRank.computePageRank` method to find the page rank for each author and venue.

With the default configuration, the page rank algorithm will run for `100` **iterations** using a **damping factor** of `0.85`. These values can be changed using the `dblp-page-rank.job.max-iterations` and `dblp-page-rank.job.damping-factor` configuration settings respectively. The number of iterations can also be specified using the *3rd* parameter passed to the `jar` file.

The output RDD from this method will contain the author/venue name as the key and their page ranks as the value.

The `DBLPPageRank.writePageRankOutput` method writes the Page Rank values computed to disk, creating separate directories for page rank values of UIC CS faculty and the publication venues. Both the lists are sorted in descending order of their page ranks.

### Results ###

#### Page Ranks for faculty of Computer Science department at UIC:

```text
Robert Sloan	0.20812250006011176
Bhaskar DasGupta	0.1865058346647529
Tanya Berger-Wolf	0.1730468043828001
Ouri Wolfson	0.16754491226824508
Barbara Di Eugenio	0.16631469239723848
Prasad Sistla	0.1652366636496375
Ugo Buy	0.16512918261459353
Philip S. Yu	0.16268042513647324
Brian Ziebart	0.16246655772258892
Chris Kanich	0.16185446154345237
Bing Liu	0.16081339184684926
Robert Kenyon	0.1602390814618725
Andrew Johnson	0.15939901665716816
Peter Nelson	0.15820967546485268
Jon Solworth	0.15615759162877835
Isabel Cruz	0.15596091578092944
Lenore Zuck	0.15570424552920614
G. Elisabeta Marai	0.15544812876970368
Jason Polakis	0.15458587641039784
V. N. Venkatakrishnan	0.1541403033289128
Luc Renambot	0.1539246838044914
Daniel J. Bernstein	0.15368705424679063
Xinhua Zhang	0.15363412037011057
Ajay Kshemkalyani	0.1534234098834733
Mark Grechanik	0.1534234098834733
Patrick Troy	0.15280800198493805
Anastasios Sidiropoulos	0.15220180499257002
Nasim Mobasheri	0.15220180499257002
```

#### Page Ranks for (Top 100) publication venues where faculty of Computer Science department at UIC have published:

```text
CoRR	0.22418167538097258
SIGCSE	0.2020377460635816
Computers and Electronics in Agriculture	0.19329560056239914
Algorithmica	0.1849269742713601
Inf. Sci.	0.1812153449818695
Discrete Applied Mathematics	0.18088411382624495
ACM Conference on Computer and Communications Security	0.17498902396818522
USENIX Security Symposium	0.17084872063927245
IEEE Symposium on Security and Privacy	0.17030713298037017
Inf. Process. Lett.	0.16665990218621965
CollaborateCom	0.16665396452525535
Theor. Comput. Sci.	0.16531620508137443
J. Comput. Syst. Sci.	0.16484856831592704
ACSAC	0.16464721957395026
PerCom Workshops	0.16433816893776776
NDSS	0.16381207674864154
ASONAM	0.16356154368035852
AISec@CCS	0.16347591657846192
SIGMOD Conference	0.16312875772749438
CODASPY	0.16269646027960077
Commun. ACM	0.16253241903154803
IEEE Trans. Systems, Man, and Cybernetics, Part A	0.16217332003974055
VR	0.16207665801241916
IJCAI	0.1620422547765723
ICSE	0.16190432698808283
SODA	0.1618208668593653
ESORICS	0.16175097602968216
COLT	0.16166444746891506
CAV	0.16153399375267624
IEEE Visualization	0.16103289289119044
IEEE Computer Graphics and Applications	0.16103289289119044
IEEE Trans. Information Theory	0.16083335298770338
STOC	0.16078663241153912
Future Generation Comp. Syst.	0.16074812685882892
Formal Methods in System Design	0.1603717151973241
AAAI	0.1603226775382695
WWW	0.16031690680661717
IEEE Trans. Software Eng.	0.16029646174342513
CHI	0.16000274894048988
CIKM	0.1599999244661671
Visualization and Data Analysis	0.15985249241817862
Advances in Computers	0.15966131757762136
Artif. Intell.	0.1594538459480899
QRS	0.1594094072151929
AsiaCCS	0.15939129977599675
ISSTA	0.1592642002033172
IEEE Trans. Parallel Distrib. Syst.	0.15926128276075255
Social Netw. Analys. Mining	0.15923244312689083
DIMVA	0.15922620033824372
SC	0.15921464893781406
Inf. Comput.	0.15915011030642082
UbiComp Adjunct	0.15908224913981422
Electronic Colloquium on Computational Complexity (ECCC)	0.15905266742262317
NIPS	0.15900081885443645
IVA	0.15877046088722538
IPDPS Workshops	0.15873346420194265
Comput. Graph. Forum	0.1585984237383807
CLUSTER	0.15854632186625892
J. Parallel Distrib. Comput.	0.15845337644754026
PDIS	0.15841752713323118
SPAA	0.15828302813736977
ICDM Workshops	0.15826595947725114
ACM Trans. Softw. Eng. Methodol.	0.15819016320729512
VMCAI	0.15811058386920296
Journal of Computer Security	0.15811058386920296
PLAS	0.15811058386920296
ICDE	0.15795480651626387
EMBC	0.15785943669962796
HVEI	0.15785943669962796
EuroVis (Short Papers)	0.15785943669962796
PVLDB	0.15771084282130543
IEEE Trans. Knowl. Data Eng.	0.15767079414723728
NSPW	0.1576573347870808
LISA	0.1576573347870808
Symposium on Computational Geometry	0.15762556937949548
ALT	0.15762158702379991
SIAM J. Discrete Math.	0.15759592667260658
FLAIRS Conference	0.15753605332454648
ICML	0.15751467555333148
ACM Comput. Surv.	0.1574486477621531
Distributed and Parallel Databases	0.1573434901372091
Encyclopedia of Cryptography and Security (2nd Ed.)	0.15731987018413052
COMPSAC	0.1572783047155009
VRAIS	0.15723088092968518
AH	0.15723088092968518
Presence	0.15723088092968518
WABI	0.15722489296878617
HPDC	0.1571816426881795
Bioinformatics	0.15715928208650898
BMC Bioinformatics	0.15715928208650898
INFOCOM	0.15705678247940527
IACR Cryptology ePrint Archive	0.15703664389063093
SPACE	0.15703664389063093
CSAW	0.15703664389063093
J. Algorithms	0.15692075209244888
SIGACT News	0.15685086243005317
Massachusetts Institute of Technology, Cambridge, MA, USA	0.15685086243005317
SSDBM	0.1568200919922103
BHI	0.15681567157839923
AISTATS	0.15679901386186645
```