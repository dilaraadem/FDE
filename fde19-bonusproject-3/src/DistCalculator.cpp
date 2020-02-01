#include "DistCalculator.hpp"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <queue>
using namespace std;

int actor_count = 1971696;
int movie_count = 1151758;
vector<vector<uint64_t>> actors;
vector<vector<uint64_t>> movies;

DistCalculator::DistCalculator(std::string edgeListFile)
{
   actors.resize(actor_count);
   movies.resize(movie_count);
   // TODO: implement graph parsing here
   ifstream ifs;   //read file here
   ifs.open(edgeListFile);
   string word;
   uint64_t actor, movie;
   getline(ifs, word); //skip the first line
   while(getline(ifs, word, ',')){//separate with comma
        actor = atoi(word.c_str()); // read actor id
        getline(ifs, word);
        movie = atoi(word.c_str()); // read movie id
        actors[actor].push_back(movie);
        movies[movie].push_back(actor);
    }
    ifs.close();
}

int64_t DistCalculator::dist(Node a, Node b)
{
   // TODO: implement distance calculation here
   if (a == b)    // if a is the same actor as b
      return 0;
   
   vector<bool> played_movies = vector<bool>(movie_count,false);
   vector<uint64_t> act_dist = vector<uint64_t>(actor_count, 0);
   queue<uint64_t> a_queue;
   a_queue.push(a);
   act_dist[a]=1;

   while(a_queue.size()){
      auto front = a_queue.front();
      a_queue.pop();
      for(auto &movie : actors[front]){
         if(!played_movies[movie]){
            played_movies[movie] = true;
            for(auto &actor: movies[movie]){
               if(actor == b){
                  return act_dist[front];
               }
               if (!act_dist[actor]) {
                  act_dist[actor] = act_dist[front] + 1;
                  a_queue.push(actor);
               }
            }
         }
      }
   }   
   return -1;
}
