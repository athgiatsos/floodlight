/**
 *    Copyright 2013, Big Switch Networks, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License"); you may
 *    not use this file except in compliance with the License. You may obtain
 *    a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
 **/

package net.floodlightcontroller.loadbalancer;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.projectfloodlight.openflow.types.U64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import net.floodlightcontroller.loadbalancer.LoadBalancer.IPClient;

/**
 * Data structure for Load Balancer based on
 * Quantum proposal http://wiki.openstack.org/LBaaS/CoreResourceModel/proposal 
 * 
 * @author KC Wang
 */


@JsonSerialize(using=LBPoolSerializer.class)
public class LBPool {
	protected static Logger log = LoggerFactory.getLogger(LBPool.class);
	protected String id;
	protected String name;
	protected String tenantId;
	protected String netId;
	protected short lbMethod;
	protected byte protocol;
	protected ArrayList<String> members;
	protected ArrayList<String> monitors;
	protected short adminState;
	protected short status;
	protected final static short ROUND_ROBIN = 1;
	protected final static short STATISTICS = 2;
	protected final static short WEIGHTED_RR = 3;
	//giatsos
	protected final static short RATIO = 4;

	protected String vipId;

	protected int previousMemberIndex;

	public LBPool() {
		id = String.valueOf((int) (Math.random()*10000));
		name = null;
		tenantId = null;
		netId = null;
		lbMethod = 0;
		protocol = 0;
		members = new ArrayList<String>();
		monitors = new ArrayList<String>();
		adminState = 0;
		status = 0;
		previousMemberIndex = -1;
	}

	public String pickMember(IPClient client, HashMap<String,U64> membersBandwidth,HashMap<String,Short> membersWeight) {
		log.info("members.size() {}",members.size());

		// Get the members that belong to this pool and the statistics for them
		if(members.size() > 0){
			log.info("lbMethod {}",lbMethod);
			log.info("membersBandwidth.isEmpty() {}",membersBandwidth.isEmpty());
			log.info("membersBandwidth.values() {}",membersBandwidth.values());
			log.info("membersWeight.isEmpty() {}",membersWeight.isEmpty());
			
			if (lbMethod == STATISTICS && !membersBandwidth.isEmpty() && membersBandwidth.values() !=null) {
				ArrayList<String> poolMembersId = new ArrayList<String>();
				for(String memberId: membersBandwidth.keySet()){
					for(int i=0;i<members.size();i++){
						if(members.get(i).equals(memberId)){
							poolMembersId.add(memberId);
							log.info("{} memberId {}", i, memberId);
						}
					}
				}
				// return the member which has the minimum bandwidth usage, out of this pool members
				if(!poolMembersId.isEmpty()){
					ArrayList<U64> bandwidthValues = new ArrayList<U64>();

					for(int j=0;j<poolMembersId.size();j++){
						bandwidthValues.add(membersBandwidth.get(poolMembersId.get(j)));
					}
					log.info("Member picked using LB statistics: {}", poolMembersId.get(bandwidthValues.indexOf(Collections.min(bandwidthValues))));
					return poolMembersId.get(bandwidthValues.indexOf(Collections.min(bandwidthValues)));
				}
				return null;
			} else if(lbMethod == WEIGHTED_RR && !membersWeight.isEmpty()){
				Random randomNumb = new Random();
				short totalWeight = 0; 

				for(Short weight: membersWeight.values()){
					totalWeight += weight;
				}
				log.info("totalWeight {}",totalWeight);
				int rand = randomNumb.nextInt(totalWeight);
				log.info("rand {}",rand);
				short val = 0;
				for(String memberId: membersWeight.keySet()){
					val += membersWeight.get(memberId);
					log.info("val {}",val);
					if(val > rand){
						log.info("Member picked using WRR: {}",memberId);
						return memberId;
					}
				}
				return null;
//giatsos
			} else if(lbMethod == RATIO && !membersWeight.isEmpty()){
				log.info("mpike!!!");
				
				Boolean computed = false;
				
				if (computed = false) {
					short totalWeight = 0; 

					for(Short weight: membersWeight.values()){
						totalWeight += weight;
					}
					log.info("totalWeight {}",totalWeight);
					if (totalWeight >10 || totalWeight <10 ) {
						//normalize
						log.info("normalizing...");
					}
					else {
						log.info("total weight OK! go on with calc");
						//gia olous tous members get id
						for(String memberId: membersWeight.keySet()){
							//create list for each member
							List membersWeight.get(memberId) = new ArrayList();
							for(int i=0; i<membersWeight.values(); i++){
								//gemizoume thn lista tou kathena me to id tou
								membersWeight.get(memberId).add(membersWeight.get(memberId));
							}
						}
					}

					//exoume listes me ta ids twn members
					//kanoume mia nea lista pou tha valoume oles tis parapanw
					List total = new ArrayList();
					//loop through all members
					for(String memberId: membersWeight.keySet()){
						//pairnoume thn lista tou kathenos kai thn prosthetoume
						total.addAll(membersWeight.get(memberId));
					}

					//anakatevoume thn lista gia na mhn einai omadopoihmenes oi anatheseis
					//http://www.vogella.com/tutorials/JavaAlgorithmsShuffle/article.html

					//print list
					for(int i = 0; i < total.size(); i++){
<<<<<<< Updated upstream
						log.info("final order {} , {}", i, total.get(i))
=======
						log.info("final order {} , {}", i, total.get(i));
>>>>>>> Stashed changes
					}

					//change flag computed, now the next 10 destinations have been decided
					computed = true;
				}
				else {
					//epistrefei ton host
					short counter = 0;
					if (computed = true){
						//log.info("Member picked using RATIO: {}",total[counter])
						//return total[counter]
						counter++;
						if (counter>10){
							computed = false;
						}
					}
				}
				return null;
//giatsos
			}else {
				// simple round robin
				previousMemberIndex = (previousMemberIndex + 1) % members.size();
				log.info("Member picked using simple round robin: {}",previousMemberIndex);
				return members.get(previousMemberIndex);
			}
		}
		return null;
	}
}
