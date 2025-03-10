import numpy as np

class Link():
    #Link between two nodes in the graph DB
    def __init__(self, origin, target, **kwargs):
        self.origin = origin
        self.target = target
        self.link_id = str(target['id'][0])

    def __eq__(self, other): #for easy comparison
        return (self.link_id == other.link_id)

    def __hash__(self): #for easy comparison
        return hash(self.link_id)

class RandomWalk():
    #Class to carry out a random walk on the graph from a startpoint following links based on their link_score

    def last_embedding(self):
        return self.embeddings[-1]

    def last_context(self):
        return self.contexts[-1]

    def last_node(self):
        return self.last_link().target

    def last_link(self):
        return self.links_traversed[-1]

    def node_context(self, node):
        return node['title'][0] + ' \n ' + node['content'][0]

    def update(self,link):
        self.links_traversed.append(link)
        self.contexts.append( (self.last_context()+'\n'  if len(self.contexts) else "")  + self.node_context(self.last_node()) )
        self.embeddings.append(self.embed(self.last_context()))


    def __init__(self, query, startpoint, embeddingfunc, query_db_func, max_steps,random_seed=42,temp=0, **kwargs):
        np.random.seed(random_seed)
        self.embed = embeddingfunc
        self.query_db = query_db_func
        self.max_steps = max_steps
        self.temp = temp
        self.query_txt = query
        self.query_vect = self.embed(query)
        #Init path
        self.links_traversed = []
        self.contexts = []
        self.embeddings = []
        self.scores = []
        self.update(Link(startpoint,startpoint))

        #Start random walk
        self.walk()


    # def get_links(self, node):
    #     #Get list of links originating from a node
    #     link_list = []
    #     if "references" in node:
    #         for link_target in node["references"][0]:
    #             target_node = self.query_db('pages', {"id": link_target["id"]}, {}, 1)
    #             if target_node.empty:
    #                 print("Broken link to:",link_target)
    #             else:
    #                 link_list.append(Link(node,target_node))
    #     return link_list

    def get_links(self, node):
        # Get list of links originating from a node
        link_list = []
        if "references" in node:
            for link_target in node["references"]:
                link_list.append(link_target.get("id"))
        return_list = []
        if len(link_list) > 0:
            df = self.query_db('pages', {"id": {"$in": link_list}}, {}, 0)
            for _, row in df.iterrows():
                return_list.append(Link(node, row))
        return return_list


    def score_link(self, link):
        #Simplest scoring
        ##score = np.dot(self.query_vect, link.target['embedding'])

        #Less simple scoring
        weight = 0.5
        ##score = weight*np.dot(self.query_vect, link.target['embedding']) + (1-weight)*np.dot(self.last_embedding(), target['embedding'])

        #expensive scoring
        orig_similarity = np.dot(self.query_vect, self.last_embedding())
        new_context_embedding = self.embed(self.last_context() + self.node_context(link.target))
        new_similarity = np.dot(self.query_vect, new_context_embedding)
        score = new_similarity - orig_similarity

        return score

    def calculate_probabilities(self, link_scores):
        #Boltzmann weights
        if (self.temp>0): #Bolztmann
            w = np.exp(np.array(link_scores)/self.temp)
        else: #full greedy
            w = np.zeros(len(link_scores))
            w[np.argmax(link_scores)] = 1.0

        w = w/np.sum(w) #normalize
        return w


    def walk(self):
        for step in range(self.max_steps):
            link_list = self.get_links(self.last_node()) #get list of links
            link_list = list(set(link_list) - set(self.links_traversed)) #remove already visited nodes to avoid recursion
            if (not len(link_list)): #break if there are no appropriate links
                break
            link_scores = [self.score_link(link) for link in link_list]
            probs = self.calculate_probabilities(link_scores)
            chosen_link_ind = np.random.choice(np.arange(len(probs)), p=probs)
            #Update
            self.scores.append(link_scores[chosen_link_ind])
            self.update(link_list[chosen_link_ind])








