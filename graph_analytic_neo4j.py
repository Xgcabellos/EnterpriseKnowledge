# version v1.1
import configparser
from logging import getLogger

from neo4j.v1 import GraphDatabase

from text_process import active_log
from text_process import log_level_conversor

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20190701'
__version__ = 0.01
__description__ = 'This script get analytic from  datasource Neo4J'

config_name = '../input/config.ini'

config = configparser.ConfigParser()
config.read(config_name)

log_name = config['LOGS']['LOG_FILE']
log_directory = config['LOGS']['LOG_DIRECTORY']
log_level = log_level_conversor(config['LOGS']['log_level_analytic_graph'])

module_logger = getLogger('GraphAnalyticNeo4j')


class Neo4jGhaphAnalytic():
    def __init__(self, driver, log_file, log_level, log_directory):
        self.driver = driver
        active_log(log_file, log_level, log_directory)
        self.logger = getLogger("Neo4jGhaphAnalytic")
        log = getLogger("neo4j.bolt")
        log.setLevel(log_level_conversor(config['LOGS']['log_level_neo4j']))
        self.open_db = True

    def analytic_type(self):
        """"Return a string representing the type of connection this is."""
        return 'graph_analytic_neo4j'

    def connect(self, url, login, pw):
        self.driver = GraphDatabase.driver(url, auth=(login, pw))
        self.self.open_db = True
        active_log(log_name, log_level, log_directory)
        self.logger = getLogger("Neo4jGhaphAnalytic")
        return self.driver

    def __del__(self):
        if self.open_db == True:
            self.driver = self.driver.close()

    def page_rank_centrality(self, tx):
        """PageRank is an algorithm that measures the transitive influence or connectivity of nodes.
        It can be computed by either iteratively distributing one node’s rank (originally based on
        degree) over its neighbours or by randomly traversing the graph and counting the frequency
        of hitting each node during these walks.
        PageRank is a variant of Eigenvector Centrality.
        PageRank can be applied across a wide range of domains.
        The following are some notable use-cases:
            Personalized PageRank is used by Twitter to present users with recommendations of other
            accounts that they may wish to follow. The algorithm is run over a graph which contains
            shared interests and common connections. Their approach is described in more detail in
            "WTF: The Who to Follow Service at Twitter".
            PageRank has been used to rank public spaces or streets, predicting traffic flow and
            human movement in these areas. The algorithm is run over a graph which contains intersections
            connected by roads, where the PageRank score reflects the tendency of people to park, or end their
            journey, on each street. This is described in more detail in "Self-organized Natural Roads for Predicting
             Traffic Flow: A Sensitivity Study".
            PageRank can be used as part of an anomaly or fraud detection system in the healthcare and
            insurance industries. It can help find doctors or providers that are behaving in an unusual
            manner, and then feed the score into a machine learning algorithm.
            https://neo4j.com/docs/graph-algorithms/3.5/algorithms/page-rank/


        """

        send_text = "CALL algo.pageRank('User', 'SEND',{iterations:20, dampingFactor:0.85, write: true," \
                    "writeProperty:'pagerank'})  YIELD nodes, iterations, loadMillis, computeMillis, writeMillis," \
                    " dampingFactor, write, writeProperty "

        for records in tx.run(send_text):
            self.logger.info('page_rank_centrality:  nodes: {}, iterations: {} , loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['iterations'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['dampingFactor'], records['write'], records['writeProperty']))

    def page_rank_centrality_w(self, tx):
        """PageRank is an algorithm that measures the transitive influence or connectivity of nodes.
        It can be computed by either iteratively distributing one node’s rank (originally based on
        degree) over its neighbours or by randomly traversing the graph and counting the frequency
        of hitting each node during these walks.
        PageRank is a variant of Eigenvector Centrality.
        PageRank can be applied across a wide range of domains.
        The following are some notable use-cases:
            Personalized PageRank is used by Twitter to present users with recommendations of other
            accounts that they may wish to follow. The algorithm is run over a graph which contains
            shared interests and common connections. Their approach is described in more detail in
            "WTF: The Who to Follow Service at Twitter".
            PageRank has been used to rank public spaces or streets, predicting traffic flow and
            human movement in these areas. The algorithm is run over a graph which contains intersections
            connected by roads, where the PageRank score reflects the tendency of people to park, or end their
            journey, on each street. This is described in more detail in "Self-organized Natural Roads for Predicting
             Traffic Flow: A Sensitivity Study".
            PageRank can be used as part of an anomaly or fraud detection system in the healthcare and
            insurance industries. It can help find doctors or providers that are behaving in an unusual
            manner, and then feed the score into a machine learning algorithm.
            https://neo4j.com/docs/graph-algorithms/3.5/algorithms/page-rank/


        """

        send_text = "CALL algo.pageRank('User', 'SEND',{iterations:20, dampingFactor:0.85, write: true," \
                    "writeProperty:'pagerank_weighted', weightProperty: 'weight'}) " \
                    "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, " \
                    "write, writeProperty "

        for records in tx.run(send_text):
            self.logger.info('page_rank_centrality_w:  nodes: {}, iterations: {} , loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['iterations'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['dampingFactor'], records['write'], records['writeProperty']))

    def page_rank_centrality_personalized(self, tx):
        """
      Personalized PageRank is a variation of PageRank which is biased towards a set of sourceNodes.
      This variant of PageRank is often used as part of recommender systems. The following examples show
       how to run PageRank centered around 'Site A'.
       https://neo4j.com/docs/graph-algorithms/3.5/algorithms/page-rank/
      :param tx:
      :return:
      """

    def Article_rank_centrality(self, tx):
        """
        ArticleRank is a variant of the PageRank algorithm, which measures the transitive influence
         or connectivity of nodes.
         Where ArticleRank differs to PageRank is that PageRank assumes that relationships from nodes
         that have a low out-degree are more important than relationships from nodes with a higher
         out-degree. ArticleRank weakens this assumption.
         https://neo4j.com/docs/graph-algorithms/3.5/algorithms/article-rank/

        :param tx:
        :return:
        """

        send_text = "CALL algo.articleRank('User', 'SEND',{iterations:20, dampingFactor:0.85, write: true," \
                    "writeProperty:'articlerank'}) " \
                    "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, " \
                    "write, writeProperty "

        for records in tx.run(send_text):
            self.logger.info('articleRank_centrality:  nodes: {}, iterations: {} , loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['iterations'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['dampingFactor'], records['write'], records['writeProperty']))

    def Article_rank_centrality_w(self, tx):
        """
        ArticleRank is a variant of the PageRank algorithm, which measures the transitive influence
         or connectivity of nodes.
         Where ArticleRank differs to PageRank is that PageRank assumes that relationships from nodes
         that have a low out-degree are more important than relationships from nodes with a higher
         out-degree. ArticleRank weakens this assumption.
         https://neo4j.com/docs/graph-algorithms/3.5/algorithms/article-rank/

        :param tx:
        :return:
        """

        send_text = "CALL algo.articleRank('User', 'SEND',{iterations:20, dampingFactor:0.85, write: true," \
                    "writeProperty:'articlerank_weighted', weightProperty: 'weight'}) " \
                    "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, " \
                    "write, writeProperty "

        for records in tx.run(send_text):
            self.logger.info('articleRank_centrality_w:  nodes: {}, iterations: {} , loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['iterations'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['dampingFactor'], records['write'], records['writeProperty']))

    def betweenness_centrality(self, tx):
        """
        The Betweenness Centrality algorithm calculates the shortest (weighted) path between every pair of
         nodes in a connected graph, using the breadth-first search algorithm. Each node receives a score,
         based on the number of these shortest paths that pass through the node. Nodes that most frequently
         lie on these shortest paths will have a higher betweenness centrality score.

        The algorithm was given its first formal definition by Linton Freeman, in his 1971 paper "A Set of
        Measures of Centrality Based on Betweenness". It was considered to be one of the "three distinct
        intuitive conceptions of centrality".


        Betweenness centrality is used to research the network flow in a package delivery process, or
        telecommunications network. These networks are characterized by traffic that has a known target and
        takes the shortest path possible. This, and other scenarios, are described by Stephen P. Borgatti
        in "Centrality and network flow".
        Betweenness centrality is used to identify influencers in legitimate, or criminal, organizations.
        Studies show that influencers in organizations are not necessarily in management positions, but instead
        can be found in brokerage positions of the organizational network. Removal of such influencers could
        seriously destabilize the organization. More detail can be found in "Brokerage qualifications in
        ringing operations", by Carlo Morselli and Julie Roy.
        Betweenness centrality can be used to help microbloggers spread their reach on Twitter, with a
        recommendation engine that targets influencers that they should interact with in the future.
         This approach is described in "Making Recommendations in a Microblog to Improve the Impact of
         a Focal User".
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/betweenness-centrality/

        :param tx:
        :return:
        """

        send_text = "CALL algo.betweenness.sampled('User','SEND', {writeProperty:'betweenness_out', write:true," \
                    " direction: 'out'}) YIELD nodes, loadMillis, computeMillis, writeMillis, " \
                    "minCentrality, maxCentrality, sumCentrality "

        for records in tx.run(send_text):
            self.logger.info('betweenness_out:  nodes: {},  loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['minCentrality'], records['maxCentrality'],
                records['sumCentrality']))

        send_text2 = "CALL algo.betweenness.sampled('User','SEND', {writeProperty:'betweenness_in', write:true," \
                     " direction: 'in'}) YIELD nodes, loadMillis, computeMillis, writeMillis, " \
                     "minCentrality, maxCentrality, sumCentrality "

        for records in tx.run(send_text2):
            self.logger.info('betweenness_in:  nodes: {},  loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['minCentrality'], records['maxCentrality'],
                records['sumCentrality']))

        send_text3 = "CALL algo.betweenness.sampled('User','SEND', {writeProperty:'betweenness', write:true," \
                     " direction: 'BOTH'}) YIELD nodes, loadMillis, computeMillis, writeMillis, " \
                     "minCentrality, maxCentrality, sumCentrality "

        for records in tx.run(send_text3):
            self.logger.info('betweenness_both:  nodes: {},  loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['minCentrality'], records['maxCentrality'],
                records['sumCentrality']))

    def betweenness_RA_centrality(self, tx):
        """
        The Betweenness Centrality algorithm calculates the shortest (weighted) path between every pair of
         nodes in a connected graph, using the breadth-first search algorithm. Each node receives a score,
         based on the number of these shortest paths that pass through the node. Nodes that most frequently
         lie on these shortest paths will have a higher betweenness centrality score.

        The algorithm was given its first formal definition by Linton Freeman, in his 1971 paper "A Set of
        Mhe RA-Brandes algorithm is the best known algorithm for calculating an approximate score for
        betweenness centrality. Rather than calculating the shortest path between every pair of nodes,
        the RA-Brandes algorithm considers only a subset of nodes. Two common strategies for selecting
        the subset of nodes are:


        random
            Nodes are selected uniformly, at random, with defined probability of selection. The default
            probability is log10(N) / e^2. If the probability is 1, then the algorithm works the same way
            as the normal Betweenness Centrality algorithm, where all nodes are loaded.
        degree
            First, the mean degree of the nodes is calculated, and then only the nodes whose degree is
            higher than the mean are visited (i.e. only dense nodes are visited).


        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/betweenness-centrality/

        :param tx:
        :return:
        """

        send_text = "CALL algo.betweenness.sampled('User','SEND', {strategy:'random', probability:1.0," \
                    " direction:'out',writeProperty:'betweenness_ra_out', write:true}) " \
                    "YIELD nodes, loadMillis, computeMillis, writeMillis, " \
                    "minCentrality, maxCentrality, sumCentrality "

        for records in tx.run(send_text):
            self.logger.info('betweenness_ra_brandes_out:  nodes: {},  loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['minCentrality'], records['maxCentrality'],
                records['sumCentrality']))

        send_text2 = "CALL algo.betweenness.sampled('User','SEND', {strategy:'random', probability:1.0," \
                     " direction:'in',writeProperty:'betweenness_ra_in', write:true}) " \
                     "YIELD nodes, loadMillis, computeMillis, writeMillis, " \
                     "minCentrality, maxCentrality, sumCentrality "

        for records in tx.run(send_text2):
            self.logger.info('betweenness_ra_brandes_in:  nodes: {},  loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['minCentrality'], records['maxCentrality'],
                records['sumCentrality']))

        send_text3 = "CALL algo.betweenness.sampled('User','SEND', {strategy:'random', probability:1.0," \
                     " direction:'BOTH',writeProperty:'betweenness_ra', write:true}) " \
                     "YIELD nodes, loadMillis, computeMillis, writeMillis, " \
                     "minCentrality, maxCentrality, sumCentrality "

        for records in tx.run(send_text3):
            self.logger.info('betweenness_ra_brandes_both:  nodes: {},  loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}, dampingFactor: {}, write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['minCentrality'], records['maxCentrality'],
                records['sumCentrality']))

    def closeness_centrality(self, tx):
        """
       Closeness centrality is a way of detecting nodes that are able to spread information very efficiently
       through a graph. The closeness centrality of a node measures its average farness (inverse distance)
       to all other nodes. Nodes with a high closeness score have the shortest distances to all other nodes.


        Closeness centrality is used to research organizational networks, where individuals with high closeness
        centrality are in a favourable position to control and acquire vital information and resources within
        the organization. One such study is "Mapping Networks of Terrorist Cells" by Valdis E. Krebs.
        Closeness centrality can be interpreted as an estimated time of arrival of information flowing through
        telecommunications or package delivery networks where information flows through shortest paths to a
        predefined target. It can also be used in networks where information spreads through all shortest paths
        simultaneously, such as infection spreading through a social network. Find more details in "Centrality
        and network flow" by Stephen P. Borgatti.
        Closeness centrality has been used to estimate the importance of words in a document, based on a graph-based
        keyphrase extraction process. This process is described by Florian Boudin in "A Comparison of Centrality
        Measures for Graph-Based Keyphrase Extraction".
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/closeness-centrality/

        :param tx:
        :return:
        """

        send_text = "CALL algo.closeness('User','SEND', {writeProperty:'closeness', write: true }) " \
                    "YIELD nodes,  loadMillis, computeMillis, writeMillis "

        for records in tx.run(send_text):
            self.logger.info('closeness:  nodes: {}, loadMillis {}, '
                             'computeMillis: {}, writeMillis: {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'], records['writeMillis']))

    def closeness_harmonic_centrality(self, tx):
        """
       Closeness centrality is a way of detecting nodes that are able to spread information very efficiently
       through a graph. The closeness centrality of a node measures its average farness (inverse distance)
       to all other nodes. Nodes with a high closeness score have the shortest distances to all other nodes.


        Harmonic centrality was proposed as an alternative to closeness centrality, and therefore
        has similar use cases. For example, we might use it if we’re trying to identify where in the city to
        place a new public service so that it’s easily accessible for residents. If we’re trying to spread a
        message on social media we could use the algorithm to find the key influencers that can help us achieve
        our goal.
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/closeness-centrality/

        :param tx:
        :return:
        """

        send_text = "CALL algo.closeness.harmonic('User','SEND', {writeProperty:'closeness_harmonic', write: true }) " \
                    "YIELD nodes,  loadMillis, computeMillis, writeMillis "

        for records in tx.run(send_text):
            self.logger.info('closenessHarmonic:  nodes: {}, loadMillis {}, computeMillis: {}, writeMillis: {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'], records['writeMillis']))

    def eigenvector_centrality(self, tx):
        """
       Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.
       Relationships to high-scoring nodes contribute more to the score of a node than connections to
       low-scoring nodes. A high score means that a node is connected to other nodes that have high scores.

       https://neo4j.com/docs/graph-algorithms/3.5/algorithms/eigenvector-centrality/
        :param tx:
        :return:
        """

        send_text = "CALL algo.eigenvector('User','SEND', {writeProperty:'eigenvector', write: true }) " \
                    "YIELD nodes,  loadMillis, computeMillis, writeMillis,writeProperty "

        for records in tx.run(send_text):
            self.logger.info('eigenvector_:  nodes: {},  loadMillis {}, computeMillis: {}, '
                             'writeMillis: {},   writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['writeProperty']))

    def eigenvector_centrality_w(self, tx):
        """
       Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.
       Relationships to high-scoring nodes contribute more to the score of a node than connections to
       low-scoring nodes. A high score means that a node is connected to other nodes that have high scores.

       https://neo4j.com/docs/graph-algorithms/3.5/algorithms/eigenvector-centrality/
        :param tx:
        :return:
        """

        send_text = "CALL algo.eigenvector('User','SEND', {writeProperty:'eigenvector_w', write: true, " \
                    "writeProperty:'weight' }) YIELD nodes,  loadMillis, computeMillis, writeMillis, writeProperty "

        for records in tx.run(send_text):
            self.logger.info('eigenvector_w_:  nodes: {},  loadMillis {}, computeMillis: {}, '
                             'writeMillis: {},   writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['writeProperty']))

    def degree_centrality(self, tx):
        """
       Degree Centrality was proposed by Linton C. Freeman in his 1979 paper Centrality in Social Networks
       Conceptual Clarification. While the Degree Centrality algorithm can be used to find the popularity
       of individual nodes, it is often used as part of a global analysis where we calculate the minimum
       degree, maximum degree, mean degree, and standard deviation across the whole graph.

        Degree centrality is an important component of any attempt to determine the most important people
        on a social network. For example, in BrandWatch’s most influential men and women on Twitter 2017
        the top 5 people in each category have over 40m followers each.

        Weighted degree centrality has been used to help separate fraudsters from legitimate users of an
        online auction. The weighted centrality for fraudsters is significantly higher because they tend
        to collude with each other to artificially increase the price of items. Read more in Two Step graph-based
        semi-supervised Learning for Online Auction Fraud Detection
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/degree-centrality/

        :param tx:
        :return:
        """

        send_text = "CALL algo.degree('User','SEND', {direction: 'incoming', writeProperty: 'followers'}) " \
                    "YIELD nodes, loadMillis, computeMillis, writeMillis, write, writeProperty"

        for records in tx.run(send_text):
            self.logger.info('degree_followers:  nodes: {},  loadMillis {}, computeMillis: {}, '
                             'writeMillis: {},  write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['write'], records['writeProperty']))

        send_text2 = "CALL algo.degree('User','SEND',  {direction: 'outgoing', writeProperty: 'followings'}) " \
                     " YIELD nodes, loadMillis, computeMillis, writeMillis, write, writeProperty "

        for records in tx.run(send_text2):
            self.logger.info('degree_followings:  nodes: {},  loadMillis {}, computeMillis: {}, '
                             'writeMillis: {},  write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['write'], records['writeProperty']))

    def degree_centrality_w(self, tx):
        """
       Degree Centrality was proposed by Linton C. Freeman in his 1979 paper Centrality in Social Networks
       Conceptual Clarification. While the Degree Centrality algorithm can be used to find the popularity
       of individual nodes, it is often used as part of a global analysis where we calculate the minimum
       degree, maximum degree, mean degree, and standard deviation across the whole graph.

        Degree centrality is an important component of any attempt to determine the most important people
        on a social network. For example, in BrandWatch’s most influential men and women on Twitter 2017
        the top 5 people in each category have over 40m followers each.

        Weighted degree centrality has been used to help separate fraudsters from legitimate users of an
        online auction. The weighted centrality for fraudsters is significantly higher because they tend
        to collude with each other to artificially increase the price of items. Read more in Two Step graph-based
        semi-supervised Learning for Online Auction Fraud Detection
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/degree-centrality/

        :param tx:
        :return:
        """

        send_text = "CALL algo.degree('User','SEND', {direction: 'incoming', writeProperty: 'weightedfollowers'," \
                    " weightProperty: 'weight'}) " \
                    "YIELD nodes, loadMillis, computeMillis, writeMillis, write, writeProperty"

        for records in tx.run(send_text):
            self.logger.info('degree_followers_w:  nodes: {},  loadMillis {}, computeMillis: {}, '
                             'writeMillis: {},  write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['write'], records['writeProperty']))

        send_text2 = "CALL algo.degree('User','SEND',  {direction: 'outgoing', writeProperty: 'weightedfollowings', " \
                     "weightProperty: 'weight'}) " \
                     " YIELD nodes, loadMillis, computeMillis, writeMillis, write, writeProperty "

        for records in tx.run(send_text2):
            self.logger.info('degree_followings_w:  nodes: {},  loadMillis {}, computeMillis: {}, '
                             'writeMillis: {},  write {}, writeProperty {}'.format(
                records['nodes'], records['loadMillis'], records['computeMillis'],
                records['writeMillis'], records['write'], records['writeProperty']))

    def louvain_community(self, tx):
        """
       The Louvain method of community detection is an algorithm for detecting communities in networks.
       It maximizes a modularity score for each community, where the modularity quantifies the quality
       of an assignment of nodes to communities by evaluating how much more densely connected the nodes
       within a community are, compared to how connected they would be in a random network.

        The Louvain algorithm is one of the fastest modularity-based algorithms, and works well with large graphs.
        It also reveals a hierarchy of communities at different scales, which can be useful for understanding the global
        functioning of a network.
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/louvain/

        :param tx:
        :return:
        """

        send_text = "CALL algo.louvain('User','SEND', {writeProperty:'community', write: true }) " \
                    "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;"

        for records in tx.run(send_text):
            self.logger.info('Louvain community:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis']))

    def hierarchical_louvain_community(self, tx):
        """
       The Louvain method of community detection is an algorithm for detecting communities in networks.
       It maximizes a modularity score for each community, where the modularity quantifies the quality
       of an assignment of nodes to communities by evaluating how much more densely connected the nodes
       within a community are, compared to how connected they would be in a random network.

        The Louvain algorithm is one of the fastest modularity-based algorithms, and works well with large graphs.
        It also reveals a hierarchy of communities at different scales, which can be useful for understanding the global
        functioning of a network.
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/louvain/

        :param tx:
        :return:
        """

        send_text = "CALL algo.louvain('User','SEND', { write:true,includeIntermediateCommunities: true," \
                    "intermediateCommunitiesWriteProperty: 'communities'}) " \
                    "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;"

        for records in tx.run(send_text):
            self.logger.info('Hierarchical Louvain community:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis']))

    def louvain_community_w(self, tx):
        """
       The Louvain method of community detection is an algorithm for detecting communities in networks.
       It maximizes a modularity score for each community, where the modularity quantifies the quality
       of an assignment of nodes to communities by evaluating how much more densely connected the nodes
       within a community are, compared to how connected they would be in a random network.

        The Louvain algorithm is one of the fastest modularity-based algorithms, and works well with large graphs.
        It also reveals a hierarchy of communities at different scales, which can be useful for understanding the global
        functioning of a network.
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/louvain/

        :param tx:
        :return:
        """

        send_text = "CALL algo.louvain('User','SEND', {weightProperty:'weight',writeProperty:'weightedcommunity'," \
                    " write: true }) YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;"

        for records in tx.run(send_text):
            self.logger.info('Louvain community:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis']))

    def hierarchical_louvain_community_w(self, tx):
        """
       The Louvain method of community detection is an algorithm for detecting communities in networks.
       It maximizes a modularity score for each community, where the modularity quantifies the quality
       of an assignment of nodes to communities by evaluating how much more densely connected the nodes
       within a community are, compared to how connected they would be in a random network.

        The Louvain algorithm is one of the fastest modularity-based algorithms, and works well with large graphs.
        It also reveals a hierarchy of communities at different scales, which can be useful for understanding the global
        functioning of a network.
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/louvain/

        :param tx:
        :return:
        """

        send_text = "CALL algo.louvain('User','SEND', { write:true,includeIntermediateCommunities: true," \
                    "intermediateCommunitiesWriteProperty: 'weightedcommunities', weightProperty:'weight'}) " \
                    "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;"

        for records in tx.run(send_text):
            self.logger.info('Hierarchical Louvain community:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis']))

    def label_propagation_segmentation_w(self, tx):
        """
        LPA is a relatively new algorithm, and was only proposed by Raghavan et al in 2007, in "Near linear
        time algorithm to detect community structures in large-scale networks". It works by propagating
        labels throughout the network and forming communities based on this process of label propagation.
         The intuition behind the algorithm is that a single label can quickly become dominant in a densely
        connected group of nodes, but will have trouble crossing a sparsely connected region. Labels will
        get trapped inside a densely connected group of nodes, and those nodes that end up with the same
        label when the algorithms finish can be considered part of the same community.
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/label-propagation/

        :param tx:
        :return:
        """

        send_text = "CALL algo.labelPropagation('User', 'SEND',{iterations:10, " \
                    "writeProperty:'weightedpartition_outgoing', " \
                    "write:true, direction: 'OUTGOING', weightProperty: 'weight'}) YIELD nodes, iterations, " \
                    "loadMillis, computeMillis, writeMillis, write, writeProperty,communityCount,didConverge"

        for records in tx.run(send_text):
            self.logger.info('Label Propagation out_w:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, didConverge {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['didConverge']))

        send_text2 = "CALL algo.labelPropagation('User', 'SEND',{iterations:10, " \
                     "writeProperty:'weightedpartition_incoming', " \
                     "write:true, direction: 'INCOMING', weightProperty: 'weight'}) YIELD nodes, iterations, " \
                     "loadMillis, computeMillis, writeMillis, write, writeProperty,communityCount,didConverge"

        for records in tx.run(send_text2):
            self.logger.info('Label Propagation in_w:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, didConverge {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['didConverge']))

        send_text3 = "CALL algo.labelPropagation('User', 'SEND',{iterations:10, " \
                     "writeProperty:'weightedpartition_both', " \
                     "write:true, direction: 'BOTH', weightProperty: 'weight'}) YIELD nodes, iterations, " \
                     "loadMillis, computeMillis, writeMillis, write, writeProperty,communityCount,didConverge"

        for records in tx.run(send_text3):
            self.logger.info('Label Propagation both_w:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, didConverge {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['didConverge']))

    def label_propagation_segmentation(self, tx):
        """
        LPA is a relatively new algorithm, and was only proposed by Raghavan et al in 2007, in "Near linear
        time algorithm to detect community structures in large-scale networks". It works by propagating
        labels throughout the network and forming communities based on this process of label propagation.
         The intuition behind the algorithm is that a single label can quickly become dominant in a densely
        connected group of nodes, but will have trouble crossing a sparsely connected region. Labels will
        get trapped inside a densely connected group of nodes, and those nodes that end up with the same
        label when the algorithms finish can be considered part of the same community.
        https://neo4j.com/docs/graph-algorithms/3.5/algorithms/label-propagation/

        :param tx:
        :return:
        """

        send_text = "CALL algo.labelPropagation('User', 'SEND',{iterations:10, writeProperty:'partition_outgoing', " \
                    "write:true, direction: 'OUTGOING'}) YIELD nodes, iterations, " \
                    "loadMillis, computeMillis, writeMillis, write, writeProperty,communityCount,didConverge"

        for records in tx.run(send_text):
            self.logger.info('Label Propagation out:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, didConverge {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['didConverge']))

        send_text2 = "CALL algo.labelPropagation('User', 'SEND',{iterations:10, writeProperty:'partition_incoming', " \
                     "write:true, direction: 'INCOMING'}) YIELD nodes, iterations, " \
                     "loadMillis, computeMillis, writeMillis, write, writeProperty,communityCount,didConverge"

        for records in tx.run(send_text2):
            self.logger.info('Label Propagation in:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, didConverge {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['didConverge']))

        send_text3 = "CALL algo.labelPropagation('User', 'SEND',{iterations:10, writeProperty:'partition_both', " \
                     "write:true, direction: 'BOTH'}) YIELD nodes, iterations, " \
                     "loadMillis, computeMillis, writeMillis, write, writeProperty,communityCount,didConverge"

        for records in tx.run(send_text3):
            self.logger.info('Label Propagation both:  nodes: {}, communityCount: {}, iterations: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, didConverge {}'.format(
                records['nodes'], records['communityCount'], records['iterations'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['didConverge']))

    def connected_components_w(self, tx):
        """
       The Connected Components, or Union Find, algorithm finds sets of connected nodes in an undirected
        graph where each node is reachable from any other node in the same set. It differs from the Strongly
        Connected Components algorithm (SCC) because it only needs a path to exist between pairs of nodes
        in one direction, whereas SCC needs a path to exist in both directions. As with SCC, UnionFind is
        often used early in an analysis to understand a graph’s structure.

        :param tx:
        :return:
        """

        send_text = "CALL algo.unionFind('User', 'SEND',{write: true, writeProperty:'weightedunion'," \
                    " weightProperty:'weight'}) YIELD nodes, communityCount, " \
                    "loadMillis, computeMillis, writeMillis, write, writeProperty "

        for records in tx.run(send_text):
            self.logger.info('unionFind_w:  nodes: {}, communityCount: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, writeProperty: {}'.format(
                records['nodes'], records['communityCount'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['writeProperty']))

    def connected_components(self, tx):
        """
       The Connected Components, or Union Find, algorithm finds sets of connected nodes in an undirected
        graph where each node is reachable from any other node in the same set. It differs from the Strongly
        Connected Components algorithm (SCC) because it only needs a path to exist between pairs of nodes
        in one direction, whereas SCC needs a path to exist in both directions. As with SCC, UnionFind is
        often used early in an analysis to understand a graph’s structure.

        :param tx:
        :return:
        """

        send_text = "CALL algo.unionFind('User', 'SEND',{write: true, writeProperty:'union'}) " \
                    "YIELD nodes, communityCount, " \
                    "loadMillis, computeMillis, writeMillis, write, writeProperty "

        for records in tx.run(send_text):
            self.logger.info('unionFind:  nodes: {}, communityCount: {}, '
                             'loadMillis {}, computeMillis: {}, writeMillis: {}, writeProperty: {}'.format(
                records['nodes'], records['communityCount'], records['loadMillis'],
                records['computeMillis'], records['writeMillis'], records['writeProperty']))

    # FALTAN!!!!

    def analysis(self):

        """
        create the ranks needed for centrality, community and similarity
        """

        GRAPHDDBB_SERVER = config['CONNECTION']['GRAPHDDBB_SERVER']

        if self.driver is None:  # or self.open_db!=True
            self.logger.error('the connection to neo4j {} is unable'.format(GRAPHDDBB_SERVER))
            return 0
        try:

            with self.driver.session() as session:
                session.read_transaction(self.page_rank_centrality)

        except Exception as read_exp:
            self.logger.error("Error in analytic() ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.page_rank_centrality_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().page_rank_centrality_w ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.page_rank_centrality_personalized)

        except Exception as read_exp:
            self.logger.error("Error in analytic().page_rank_centrality_personalized ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.Article_rank_centrality)

        except Exception as read_exp:
            self.logger.error("Error in analytic().Article_rank_centrality ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.Article_rank_centrality_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().Article_rank_centrality_w ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.betweenness_centrality)

        except Exception as read_exp:
            self.logger.error("Error in analytic().betweenness_centrality ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.closeness_centrality)

        except Exception as read_exp:
            self.logger.error("Error in analytic().closeness_centrality ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.closeness_harmonic_centrality)

        except Exception as read_exp:
            self.logger.error("Error in analytic().closeness_harmonic_centrality ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.eigenvector_centrality)

        except Exception as read_exp:
            self.logger.error("Error in analytic().eigenvector_centrality ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.eigenvector_centrality_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().eigenvector_centrality_w ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.degree_centrality)

        except Exception as read_exp:
            self.logger.error("Error in analytic().degree_centrality ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.degree_centrality_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().degree_centrality_w ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.louvain_community)

        except Exception as read_exp:
            self.logger.error("Error in analytic().louvain_community ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.louvain_community_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().louvain_community_w ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.hierarchical_louvain_community)

        except Exception as read_exp:
            self.logger.error("Error in analytic().hierarchical_louvain_community ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.hierarchical_louvain_community_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().hierarchical_louvain_community_w ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.label_propagation_segmentation)

        except Exception as read_exp:
            self.logger.error("Error in analytic().label_propagation_segmentation ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.label_propagation_segmentation_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().label_propagation_segmentation_w ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.connected_components)

        except Exception as read_exp:
            self.logger.error("Error in analytic().connected_components ", read_exp)

        try:

            with self.driver.session() as session:
                session.read_transaction(self.connected_components_w)

        except Exception as read_exp:
            self.logger.error("Error in analytic().connected_components_w ", read_exp)

        self.logger.info('End of graph basic analytic')


def main():
    module_logger.info('Executing  graphs algorithms functionality.')
    module_logger.info(
        'Starting neo4j graph analytic  v.' + str(__version__) + '  ' + __description__ + '  by  ' + _author__)

    driver = None
    GRAPHDDBB_SERVER = config['CONNECTION']['GRAPHDDBB_SERVER']
    try:
        driver = GraphDatabase.driver("bolt://" + GRAPHDDBB_SERVER + ":7687",
                                      auth=(config['CONNECTION']['GRAPHDDBB_LOGIN'],
                                            config['CONNECTION']['GRAPHDDBB_PASSWORD']))
    except Exception as bErr:
        module_logger.error('Error connecting GraphDatabase: %s', bErr)

    graph = Neo4jGhaphAnalytic(driver, log_name, log_level, log_directory)
    graph.analysis()


if __name__ == '__main__':
    main()
