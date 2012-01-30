
/*
 * Copyright 2011 Miguel Mendoza - miguel@micovery.com
 *
 * Insane Balancer is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the License, 
 * or (at your option) any later version. Insane Balancer is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of the 
 * GNU General Public License along with Insane Balancer. If not, see http://www.gnu.org/licenses/.
 * 
 */


using System;
using System.IO;
using System.Text;
using System.Reflection;
using System.Collections.Generic;
using System.Collections;
using System.Net;
using System.Threading;
using System.Diagnostics;


using System.Web;
using System.Data;
using System.Text.RegularExpressions;


using PRoCon.Core;
using PRoCon.Core.Plugin;
using PRoCon.Core.Plugin.Commands;
using PRoCon.Core.Players;
using PRoCon.Core.Players.Items;
using PRoCon.Core.Battlemap;
using PRoCon.Core.Maps;





namespace PRoConEvents
{ 
    public class InsaneBalancer : PRoConPluginAPI, IPRoConPluginInterface
    {

        int check_state_phase = 0;
        int login_state = 0;
        bool delayed_connect = false;
        int win_teamId, lose_teamId;
        public bool round_balancer = false;
        public bool live_balancer = false;
        public bool level_started = false;
        public bool wait_state = false;
        bool sleep = false;
        bool virtual_mode = false;
        int max_player_count = 0;
        public BattleLog blog = null;
        public int attempts = 0;
        int min_tickets = 0;





        public class BattleLog
        {

            private String gate = "https://battlelog.battlefield.com/bf3/gate";
            private HttpWebRequest req = null;
            private CookieContainer cookies = null;
            private InsaneBalancer plugin = null;
            WebClient client = null;


            public BattleLog(InsaneBalancer plugin)
            {
                this.plugin = plugin;
            }

           


            private String fetchWebPage(ref String html_data, String url)
            {
                try
                {
    				if (client == null)
					client = new WebClient();

					html_data = client.DownloadString(url);
					return html_data;


                }
                catch (WebException e)
                {
                    if (e.Status.Equals(WebExceptionStatus.Timeout))
                        throw new StatsException("^1^bERROR^n^0: HTTP request timed-out");
                    else
                        throw;
                        
                }

                return html_data;


            }

            public class StatsException : Exception
            {
                public StatsException(String message)
                    : base(message)
                {
                }
            }

            public void extractClanTag(ref String result, PlayerStats stats, String name)
            {
                /* Extract the player tag */
                Match tag = Regex.Match(result, @"\[\s*([a-zA-Z0-9]+)\s*\]\s*" + name, RegexOptions.IgnoreCase | RegexOptions.Singleline);
                if (tag.Success)
                    stats.tag = tag.Groups[1].Value;
            }

            public PlayerStats getPlayerStats(String player)
            {
                try
                {
                    String result = "";

                    /* First fetch the player's main page to get the persona id */
                    fetchWebPage(ref result, "http://battlelog.battlefield.com/bf3/user/" + player);
                 
                    /* Extract the persona id */
                    Match pid = Regex.Match(result, @"bf3/soldier/" + player + @"/stats/(\d+)", RegexOptions.IgnoreCase | RegexOptions.Singleline);

                    if (!pid.Success)
                        throw new StatsException("^1^bERROR^n^0: could not find persona-id for ^b" + player);

                    String personaId = pid.Groups[1].Value.Trim();

                    // extract the player clan-tag
                    PlayerStats ps = new PlayerStats();
                    extractClanTag(ref result, ps, player);

                    // follow link to the player's detailed stats
                    fetchWebPage(ref result, "http://battlelog.battlefield.com/bf3/overviewPopulateStats/" + personaId + "/bf3-us-engineer/1/");

                    Hashtable json = (Hashtable)JSON.JsonDecode(result);

                    if (json == null)
                        throw new StatsException("^1^bERROR^0^n: could not parse JSON profile data for ^b"+player+"^n");

                    // check we got a valid response

                    if (!(json.ContainsKey("type") && json.ContainsKey("message")))
                        throw new StatsException("^1^bERROR^0^n: JSON response does not contain \"type\" or \"message\" fields");

                    String type = (String)json["type"];
                    String message = (String)json["message"];


                    if (!(type.StartsWith("success") && message.StartsWith("OK")))
                        throw new StatsException("^1^bERROR^0^n: JSON response was ^btype^n=^b" + type + "^b, ^bmessage^n=^b" + message);


                    Hashtable data = null;
                    if (!json.ContainsKey("data") || (data = (Hashtable)json["data"]) == null)
                        throw new StatsException("^1^bERROR^0^n: JSON response was does not contain a ^bdata^n field");

                    Hashtable stats = null;
                    if (!data.ContainsKey("overviewStats") || (stats = (Hashtable)data["overviewStats"]) == null)
                        throw new StatsException("^1^bERROR^0^n: JSON response ^bdata^n does not contain ^boverviewStats^n");


                    // get the data fields
                    if (stats.ContainsKey("kills"))
                        Double.TryParse(stats["kills"].ToString(), out ps.kills);

                    if (stats.ContainsKey("elo"))
                        Double.TryParse(stats["elo"].ToString(), out ps.skill);

                    if (stats.ContainsKey("deaths"))
                        Double.TryParse(stats["deaths"].ToString(), out ps.deaths);

                    if (stats.ContainsKey("kdRatio"))
                        Double.TryParse(stats["kdRatio"].ToString(), out ps.kdr);

                    if (stats.ContainsKey("rank"))
                        Double.TryParse(stats["rank"].ToString(), out ps.rank);

                    if (stats.ContainsKey("scorePerMinute"))
                        Double.TryParse(stats["scorePerMinute"].ToString(), out ps.spm);

                    if (stats.ContainsKey("quitPercentage"))
                        Double.TryParse(stats["quitPercentage"].ToString(), out ps.quits);

                    if (stats.ContainsKey("totalScore"))
                        Double.TryParse(stats["totalScore"].ToString(), out ps.score);

                    if (stats.ContainsKey("accuracy"))
                        Double.TryParse(stats["accuracy"].ToString(), out ps.accuracy);

                    if (stats.ContainsKey("timePlayed"))
                        Double.TryParse(stats["timePlayed"].ToString(), out ps.secs);

                    return ps;


                }
                catch (StatsException e)
                {
                    plugin.ConsoleWrite(e.Message);
                }
                catch (Exception e)
                {
                    plugin.dump_exception(e);
                }

                return new PlayerStats();

            }
        }


        private class PlayerSquad
        {
            public List<PlayerProfile> members;
            int squadId = 0;
            int teamId = 0;
            int random_value = -1;

            public PlayerSquad(int tid, int sid)
            {
                members = new List<PlayerProfile>();
                teamId = tid;
                squadId = sid;
            }

            public PlayerSquad(PlayerSquad squad)
            {
                squadId = squad.squadId;
                teamId = squad.squadId;
                members = squad.members;
            }

            private bool playerBelongs(PlayerProfile player)
            {
                return getTeamId() == player.getTeamId() && getSquadId() == player.getSquadId();
            }

            public bool hasPlayer(PlayerProfile player)
            {
                return members.Contains(player);
            }

            public int getFreeSlots()
            {
                if (getSquadId() == 0)
                    return 32;

                return 4 - getCount();
            }

            public bool addPlayer(PlayerProfile player)
            {
                if (!playerBelongs(player) || hasPlayer(player))
                    return false;

                members.Add(player);
                return true;
            }

            public virtual int getTeamId()
            {
                return teamId;
            }

            public virtual int getSquadId()
            {
                return squadId;
            }


            public bool dropPlayer(PlayerProfile player)
            {
                if (!members.Contains(player))
                    return false;

                members.Remove(player);
                return true;
            }



            public virtual PlayerProfile getRandomPlayer()
            {
                int pcount = getCount();
                if (pcount == 0)
                    return null;

                return getMembers()[(new Random()).Next(pcount)];
            }

            public virtual int getRandomValue()
            {
                if (random_value == -1)
                    random_value = (new Random()).Next(0, int.MaxValue);
                return random_value;
            }

            public PlayerProfile removeRandomPlayer()
            {
                PlayerProfile player = getRandomPlayer();
                if (player == null)
                    return null;

                dropPlayer(player);
                return player;
            }

            public void sortMembers(player_sort_method sort_method)
            {
                this.members.Sort(new Comparison<PlayerProfile>(sort_method));

            }

            public int getCount()
            {
                return members.Count;
            }

            public List<PlayerProfile> getMembers()
            {
                return members;
            }

            /* Round Level statistics */

            public double getRoundSpm()
            {
                int count = getCount();
                if (count == 0)
                    return 0;

                double spm = 0;
                foreach (PlayerProfile player in getMembers())
                {
                    spm += player.getRoundSpm();
                }

                return (spm / count);
            }

            public double getRoundKpm()
            {
                int count = getCount();
                if (count == 0)
                    return 0;

                double kpm = 0;
                foreach (PlayerProfile player in getMembers())
                {
                    kpm += player.getRoundKpm();
                }

                return (kpm / count);
            }


            public double getRoundKills()
            {
                int count = getCount();
                if (count == 0)
                    return 0;

                double kills = 0;
                foreach (PlayerProfile player in getMembers())
                {
                    kills += player.getRoundKills();
                }

                return (kills / count);
            }

            public double getRoundDeaths()
            {
                int count = getCount();
                if (count == 0)
                    return 0;

                double deaths = 0;
                foreach (PlayerProfile player in getMembers())
                {
                    deaths += player.getRoundDeaths();
                }

                return (deaths / count);
            }

            public double getRoundScore()
            {
                int count = getCount();
                if (count == 0)
                    return 0;

                double score = 0;
                foreach (PlayerProfile player in getMembers())
                {
                    score += player.getRoundScore();
                }

                return (score / count);
            }


            public double getRoundKdr()
            {
                return (getRoundKills() + 1) / (getRoundDeaths() + 1);
            }

            public DateTime getRoundTime()
            {
                int count = getCount();
                if (count == 0)
                    return DateTime.Now;

                long total_ticks = 0;
                foreach (PlayerProfile player in getMembers())
                {
                    total_ticks += player.getRoundTime().Ticks;
                }

                long avg = total_ticks / count;
                return new DateTime(avg);
            }

            /* Online statistics */


            public double getOnlineSkill()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double skill = 0;
                foreach (PlayerProfile player in getMembers())
                    skill += player.getOnlineSkill();

                return (skill / count);
            }

            public double getOnlineSpm()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double spm = 0;
                foreach (PlayerProfile player in getMembers())
                    spm += player.getOnlineSpm();

                return (spm / count);
            }

            public double getOnlineKpm()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double kpm = 0;
                foreach (PlayerProfile player in getMembers())
                    kpm += player.getOnlineKpm();

                return (kpm / count);
            }


            public double getOnlineKills()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double kills = 0;
                foreach (PlayerProfile player in getMembers())
                    kills += player.getOnlineKills();

                return (kills / count);
            }

            public double getOnlineDeaths()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double deaths = 0;
                foreach (PlayerProfile player in getMembers())
                    deaths += player.getOnlineDeaths();

                return (deaths / count);
            }

            public double getOnlineScore()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double score = 0;
                foreach (PlayerProfile player in getMembers())
                    score += player.getOnlineScore();

                return (score / count);
            }


            public double getOnlineKdr()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double kdr = 0;
                foreach (PlayerProfile player in getMembers())
                    kdr += player.getOnlineKdr();

                return (kdr / count);
            }

            public double getOnlineRank()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double rank = 0;
                foreach (PlayerProfile player in getMembers())
                    rank += player.getOnlineRank();

                return (rank / count);
            }

            public double getOnlineQuits()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double quits = 0;
                foreach (PlayerProfile player in getMembers())
                    quits += player.getOnlineQuits();

                return (quits / count);
            }

            public double getOnlineAccuracy()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double accuracy = 0;
                foreach (PlayerProfile player in getMembers())
                    accuracy += player.getOnlineAccuracy();

                return (accuracy / count);
            }


            public double getOnlineTime()
            {
                double count = getCount();
                if (count == 0)
                    return 0;

                double total_ticks = 0;
                foreach (PlayerProfile player in getMembers())
                    total_ticks += (double)player.getOnlineTime();

                return total_ticks / count;

            }


            public override String ToString()
            {
                return "Team(" + TN(getTeamId()) + ").Squad(" + SQN(getSquadId()) + "): " + getMembersListStr();
            }

            public string getMembersListStr()
            {
                List<string> names = new List<string>();
                foreach (PlayerProfile player in members)
                    names.Add(player.ToString());

                return String.Join(", ", names.ToArray());
            }


            public string getClanTag()
            {
                if (getCount() == 0)
                    return "";

                return getRandomPlayer().getClanTag();
            }

            public string getMajorityClanTag()
            {

                if (getCount() == 0)
                    return "";

                Dictionary<string, int> tagCount = new Dictionary<string, int>();
                string tag = "";

                /* count how many times each tag repeats */
                foreach (PlayerProfile player in getMembers())
                {
                    tag = player.getClanTag();
                    if (tag == null || tag.Length == 0)
                        continue;

                    if (!tagCount.ContainsKey(tag))
                        tagCount[tag] = 0;

                    tagCount[tag]++;
                }

                if (tagCount.Count == 0)
                    return "";

                /* sort by ascending tag count */
                List<KeyValuePair<string, int>> list = new List<KeyValuePair<string, int>>(tagCount);
                list.Sort(delegate(KeyValuePair<string, int> left, KeyValuePair<string, int> right) { return left.Value.CompareTo(right.Value) * (-1); });

                return list[0].Key;
            }

            public void setSquadId(int sid)
            {
                squadId = sid;
            }

            public void setTeamId(int tid)
            {
                teamId = tid;
            }
        }

        private class PlayerMessage
        {
            public MessageType type;
            public string text;
            public int time;

            public PlayerMessage(string tx)
            {
                text = tx;
                type = MessageType.say;
            }
        }

        public enum PluginState { stop, wait, check, warn, balance };

        //variables to keep track of the start time for each state
        DateTime startStopTime;
        DateTime startWaitTime;
        DateTime startCheckTime;
        DateTime startWarnTime;
        DateTime startBalanceTime;
        DateTime startRoundTime;
        DateTime utc; //universal time;

        PluginState pluginState;
        CServerInfo serverInfo = null;

        bool plugin_enabled = false;

        public Dictionary<String, String> maps;
        public Dictionary<String, String> modes;
        public Dictionary<String, List<String>> settings_group;
        public Dictionary<String, int> settings_group_order;
        public Dictionary<string, bool> booleanVariables;
        public Dictionary<string, int> integerVariables;
        public Dictionary<string, float> floatVariables;
        public Dictionary<string, string> stringListVariables;
        public Dictionary<string, string> stringVariables;
        public List<string> hiddenVariables;


        public delegate bool integerVariableValidator(string var, int value);
        public delegate bool booleanVariableValidator(string var, bool value);
        private delegate int player_sort_method(PlayerProfile left, PlayerProfile right);
        private delegate int squad_sort_method(PlayerSquad left, PlayerSquad right);
        public delegate bool stringVariableValidator(string var, string value);
        public Dictionary<string, integerVariableValidator> integerVarValidators;
        public Dictionary<string, booleanVariableValidator> booleanVarValidators;
        public Dictionary<string, stringVariableValidator> stringVarValidators;


        private Dictionary<string, PlayerProfile> players;
        Dictionary<String, CPunkbusterInfo> new_player_queue;
        EventWaitHandle wake_handle;

        Thread stats_fetching_thread;


        public enum PlayerState { dead, alive, left, kicked, limbo };
        public enum MessageType { say, invalid };


        public class PlayerStats
        {
            public double rank;
            public double kills;
            public double deaths;
            public double score;
            public double skill;
            public double accuracy;
            public double quits;
            public double kdr;
            public double spm;
            public string tag = "";
            public double secs;

            public void reset()
            {
                rank = 0;
                kills = 0;
                deaths = 0;
                score = 0;
                skill = 0;
                accuracy = 0;
                quits = 0;
                kdr = 0;
                tag = "";
                secs = 0;
            }

        }

        private class PlayerProfile
        {
            private InsaneBalancer plugin;
            public string name;
            public string tag = "";
            public PlayerStats stats;
            public PlayerStats round_stats;

            public PlayerState state;
            public CPlayerInfo info;
            public CPunkbusterInfo pbinfo;
            public Queue<PlayerMessage> qmsg;          //queued messages 
            public DateTime time = DateTime.Now;
            public DateTime last_kill = DateTime.Now;
            public DateTime last_death = DateTime.Now;
            public DateTime last_spawn = DateTime.Now;
            public DateTime last_chat = DateTime.Now;
            public DateTime last_score = DateTime.Now;


            public int savedTeamId = -1;
            public int savedSquadId = -1;

            public int targetTeamId = -1;
            public int targetSquadId = -1;

            public int delayedTeamId = -1;
            public int delayedSquadId = -1;

            public int random_value = -1;


            public bool isInGame()
            {
                return (info.TeamID >= 0);
            }

            public PlayerProfile(PlayerProfile player)
            {
                /* shallow copy */
                updateInfo(player.info);
                pbinfo = player.pbinfo;
                name = player.name;
                plugin = player.plugin;
                stats = player.stats;
                state = player.state;
                qmsg = player.qmsg;
                tag = player.tag;
                time = player.time;
                random_value = player.random_value;

                last_kill = player.last_kill;
                last_death = player.last_death;
                last_spawn = player.last_spawn;
                last_chat = player.last_chat;
                last_score = player.last_score;

                savedTeamId = player.savedTeamId;
                savedSquadId = player.savedSquadId;

                targetTeamId = player.targetTeamId;
                targetSquadId = player.targetSquadId;

                delayedTeamId = player.delayedTeamId;
                delayedSquadId = player.delayedSquadId;
            }

            public PlayerProfile(InsaneBalancer plg, CPunkbusterInfo inf)
            {

                try
                {
                    plugin = plg;
                    info = new CPlayerInfo();
                    pbinfo = inf;
                    name = pbinfo.SoldierName;

                    time = DateTime.Now;
                    round_stats = new PlayerStats();
                    stats = new PlayerStats();
                    resetStats();

                    fetchStats();
                }
                catch (Exception e)
                {
                    plugin.dump_exception(e);
                }
            }

            public void fetchStats()
            {
                stats = plugin.blog.getPlayerStats(name);
                plugin.ConsoleWrite(this.ToString() + ", fetched, " + getOnlineStatistics());
            }


            public void updateInfo(CPlayerInfo inf)
            {
                /* don't update the information while round is begining */
                if (plugin.round_balancer)
                    return;

                if (plugin.live_balancer)
                    return;


                if (info.Score != inf.Score)
                    updateLastScore();

                info = inf;

                round_stats.kills = info.Kills;
                round_stats.deaths = info.Deaths;
                round_stats.score = info.Score;
            }


            public void resetStats()
            {
                //queued messages
                qmsg = new Queue<PlayerMessage>();

                //other
                state = PlayerState.limbo;

                round_stats.reset();

                savedSquadId = -1;
                savedTeamId = -1;
                targetSquadId = -1;
                targetTeamId = -1;
                delayedTeamId = -1;
                delayedSquadId = -1;
                random_value = -1;

                last_kill = DateTime.Now;
                last_chat = DateTime.Now;
                last_death = DateTime.Now;
                last_spawn = DateTime.Now;
                last_score = DateTime.Now;
            }


            /* Player Messages */
            public void dequeueMessages()
            {
                while (this.qmsg.Count > 0)
                {
                    PlayerMessage msg = this.qmsg.Dequeue();
                    if (msg.type.Equals(MessageType.say))
                    {
                        this.plugin.SendPlayerMessage(name, msg.text);
                    }
                }
            }


            public void enqueueMessage(PlayerMessage msg)
            {
                this.qmsg.Enqueue(msg);
            }


            public bool willMoveAcrossTeams()
            {
                return willMoveTeams();
            }


            public bool willMoveAcrossSquad()
            {
                return !willMoveAcrossTeams() && willMoveSquads();
            }

            public bool willMoveTeams()
            {
                return getSavedSquadId() != getTargetTeamId();
            }

            public bool willMoveSquads()
            {
                return getSavedSquadId() != getTargetSquadId();
            }


            /* Round Level Statistics */
            public double getRoundKpm()
            {
                double minutes = plugin.getRoundMinutes();
                double kills = getRoundKills();

                return kills / minutes;
            }

            public double getRoundSpm()
            {
                double minutes = plugin.getRoundMinutes();
                double score = getRoundScore();
                return score / minutes;
            }

            public double getRoundKills()
            {
                return round_stats.kills;
            }

            public double getRoundScore()
            {
                return round_stats.score;
            }

            public double getRoundDeaths()
            {
                return round_stats.deaths;
            }

            public double getRoundKdr()
            {
                return (getRoundKills() + 1) / (getRoundDeaths() + 1);

            }





            /* Online  Statistics */
            public double getOnlineKpm()
            {
                double minutes = getOnlineTime();
                double kills = getOnlineKills();

                if (kills == 0 || minutes == 0)
                    return 0;

                return kills / minutes;
            }

            public double getOnlineSpm()
            {
                double minutes = getOnlineTime();
                double score = getOnlineScore();

                if (score == 0 || minutes == 0)
                    return 0;

                return score / minutes;
            }

            public double getOnlineKills()
            {
                return stats.kills;
            }

            public double getOnlineScore()
            {
                return stats.score;
            }

            public double getOnlineDeaths()
            {
                return stats.deaths;
            }

            public double getOnlineKdr()
            {
                return stats.kdr;
            }

            public double getOnlineTime()
            {
                return stats.secs / 60D;
            }

            public double getOnlineRank()
            {
                return stats.rank;
            }

            public double getOnlineQuits()
            {
                return stats.quits;
            }

            public double getOnlineAccuracy()
            {
                return stats.accuracy;
            }

            public double getOnlineSkill()
            {
                return stats.skill;
            }


            public DateTime getRoundTime()
            {
                return time;
            }

            public virtual int getRandomValue()
            {
                if (random_value == -1)
                    random_value = (new Random()).Next(0, int.MaxValue);
                return random_value;
            }

            public override string ToString()
            {
                string t = getClanTag();
                if (t.Length > 0)
                    return "[" + t + "]" + name;
                return name;
            }


            /* Player State and Information */
            public bool isAlive()
            {
                return state.Equals(PlayerState.alive);
            }

            public bool isDead()
            {
                return state.Equals(PlayerState.dead);
            }

            public bool wasKicked()
            {
                return state.Equals(PlayerState.kicked);
            }

            public bool leftGame()
            {
                return state.Equals(PlayerState.left);
            }


            public string getClanTag()
            {
                return stats.tag;
            }

            public bool isInClan()
            {
                return getClanTag().Length > 0;
            }

            public int getLastScore()
            {
                return (int)DateTime.Now.Subtract(last_score).TotalSeconds;
            }

            public int getLastChat()
            {
                return (int)DateTime.Now.Subtract(last_chat).TotalSeconds;
            }

            public void updateLastScore()
            {
                last_score = DateTime.Now;
            }

            public void updateLastChat()
            {
                last_chat = DateTime.Now;
            }

            public int getLastKill()
            {
                return (int)DateTime.Now.Subtract(last_kill).TotalSeconds;
            }

            public void updateLastKill()
            {
                last_kill = DateTime.Now;
            }

            public int getLastDeath()
            {
                return (int)DateTime.Now.Subtract(last_death).TotalSeconds;
            }

            public void updateLastDeath()
            {
                last_death = DateTime.Now;
            }


            public int getLastSpawn()
            {
                return (int)DateTime.Now.Subtract(last_spawn).TotalSeconds;
            }




            public void updateLastSpawn()
            {
                last_spawn = DateTime.Now;
            }



            public virtual void setSquadId(int sid)
            {
                info.SquadID = sid;
            }

            public virtual void setTeamId(int tid)
            {
                info.TeamID = tid;
            }

            public virtual int getSquadId()
            {
                return info.SquadID;
            }

            public virtual int getTeamId()
            {
                return info.TeamID;
            }

            public void saveTeamSquad()
            {

                if (savedTeamId == -1)
                    savedTeamId = getTeamId();

                if (savedSquadId == -1)
                    savedSquadId = getSquadId();
            }

            public void saveTargetTeamSquad()
            {

                if (targetTeamId == -1)
                    targetTeamId = getTeamId();

                if (targetSquadId == -1)
                    targetSquadId = getSquadId();
            }

            public void saveDelayedTeamSquad()
            {
                if (delayedTeamId == -1)
                    delayedTeamId = getTeamId();

                if (delayedSquadId == -1)
                    delayedSquadId = getSquadId();
            }

            public void resetDelayedTeamSquad()
            {
                delayedTeamId = -1;
                delayedSquadId = -1;
            }

            public void resetSavedTeamSquad()
            {
                savedTeamId = -1;
                savedSquadId = -1;
            }

            public int getSavedSquadId()
            {
                return savedSquadId;
            }

            public int getSavedTeamId()
            {
                return savedTeamId;
            }

            public int getTargetSquadId()
            {
                return targetSquadId;
            }

            public int getTargetTeamId()
            {
                return targetTeamId;
            }

            public int getDelayedTeamId()
            {
                return delayedTeamId;
            }


            public int getDelayedSquadId()
            {
                return delayedSquadId;
            }


            public void resetTeamSquad()
            {
                if (savedTeamId > 0)
                    setTeamId(savedTeamId);

                if (savedSquadId > 0)
                    setSquadId(savedSquadId);
            }


            public string getRoundStatistics()
            {
                return String.Format("score({0}), kills({1}), deaths({2}) kdr({3}), spm({4}), kpm({5}), time({6})", getRoundScore(), getRoundKills(), getRoundDeaths(), Math.Round(getRoundKdr(), 2), Math.Round(getRoundSpm(), 2), Math.Round(getRoundKpm(), 2), getRoundTime());
            }

            public string getOnlineStatistics()
            {
                return String.Format("score({0}), rank({1}), skill({2}), kills({3}), deaths({4}) kdr({5}), spm({6}), kpm({7}), quits({8}), acc({9})", getOnlineScore(), getOnlineRank(), Math.Round(getOnlineSkill(), 2), getOnlineKills(), getOnlineDeaths(), Math.Round(getOnlineKdr(), 2), Math.Round(getOnlineSpm(), 2), Math.Round(getOnlineKpm(), 2), Math.Round(getOnlineQuits(), 2), Math.Round(getOnlineAccuracy(), 2));
            }


            public string getIdleStatistics()
            {
                return String.Format("last_kill({0}), last_death({1}), last_spawn({2}) last_chat({3}) last_score({4})", getLastKill(), getLastDeath(), getLastSpawn(), getLastChat(), getLastScore());
            }

        }


        public InsaneBalancer()
        {
            utc = DateTime.Now;
            startRoundTime = utc;
            blog = new BattleLog(this);

            this.maps = new Dictionary<string, string>();
            maps.Add("mp_001", "grand_bazaar");
            maps.Add("mp_003", "teheran_highway");
            maps.Add("mp_007", "caspian_border");
            maps.Add("mp_011", "seine_crossing");
            maps.Add("mp_012", "operation_firestorm");
            maps.Add("mp_013", "damavand_peak");
            maps.Add("mp_017", "noshahar_canals");
            maps.Add("mp_018", "kharg_island");
            maps.Add("mp_subway", "operation_metro");

            maps.Add("xp1_001", "strike_karkand");
            maps.Add("xp1_002", "gulf_oman");
            maps.Add("xp1_003", "sharqi_peninsula");
            maps.Add("xp1_004", "wake_island");

            this.modes = new Dictionary<string, string>();
            modes.Add("conquestlarge0", "cl");
            modes.Add("conquestsmall0", "cs");
            modes.Add("conquestsmall1", "csa");
            modes.Add("rushlarge0", "rl");
            modes.Add("teamdeathmatch0", "td");
            modes.Add("squadrush0", "sr");




            this.players = new Dictionary<string, PlayerProfile>();
            this.new_player_queue = new Dictionary<string, CPunkbusterInfo>();
            
            


            this.booleanVariables = new Dictionary<string, bool>();
            this.booleanVariables.Add("auto_start", true);
            this.booleanVariables.Add("keep_squads_live", true);
            this.booleanVariables.Add("keep_squads_round", true);
            this.booleanVariables.Add("keep_clans_live", false);
            this.booleanVariables.Add("keep_clans_round", false);
            this.booleanVariables.Add("use_white_list", false);
            this.booleanVariables.Add("use_extra_white_lists", false);
            this.booleanVariables.Add("virtual_mode", false);
            this.booleanVariables.Add("warn_say", true);
            this.booleanVariables.Add("balance_round", true);
            this.booleanVariables.Add("balance_live", true);
            this.booleanVariables.Add("kick_idle", true);
            this.booleanVariables.Add("wait_death", false);


            this.booleanVariables.Add("quiet_mode", false);
            this.booleanVariables.Add("advanced_mode", false);



            this.integerVariables = new Dictionary<string, int>();
            this.integerVariables.Add("wait_death_count", 6);
            this.integerVariables.Add("balance_threshold", 1);
            this.integerVariables.Add("debug_level", 3);
            this.integerVariables.Add("live_interval_time", 15);
            this.integerVariables.Add("round_interval", 1);
            this.integerVariables.Add("round_wait_time", 3);
            this.integerVariables.Add("warn_msg_interval_time", 15);
            this.integerVariables.Add("warn_msg_total_time", 15);
            this.integerVariables.Add("warn_msg_countdown_time", 3);
            this.integerVariables.Add("warn_msg_display_time", 5);

            this.integerVariables.Add("last_kill_time", 300);
            this.integerVariables.Add("last_death_time", 300);
            this.integerVariables.Add("last_spawn_time", 300);
            this.integerVariables.Add("last_chat_time", 300);
            this.integerVariables.Add("last_score_time", 300);
            this.integerVariables.Add("ticket_threshold", 0);


            this.integerVarValidators = new Dictionary<string, integerVariableValidator>();
            this.integerVarValidators.Add("warn_msg_interval_time", integerValidator);
            this.integerVarValidators.Add("warn_msg_total_time", integerValidator);
            this.integerVarValidators.Add("warn_msg_display_time", integerValidator);
            this.integerVarValidators.Add("warn_msg_countdown_time", integerValidator);
            this.integerVarValidators.Add("balance_threshold", integerValidator);
            this.integerVarValidators.Add("round_interval", integerValidator);
            this.integerVarValidators.Add("live_interval_time", integerValidator);
            this.integerVarValidators.Add("debug_level", integerValidator);
            this.integerVarValidators.Add("last_kill_time", integerValidator);
            this.integerVarValidators.Add("last_death_time", integerValidator);
            this.integerVarValidators.Add("last_spawn_time", integerValidator);
            this.integerVarValidators.Add("last_chat_time", integerValidator);
            this.integerVarValidators.Add("last_score_time", integerValidator);
            this.integerVarValidators.Add("ticket_threshold", integerValidator);
            this.integerVarValidators.Add("wait_death_count", integerValidator);

            /* set up per-map intervals */
            List<String> map_interval = new List<string>();
            foreach (KeyValuePair<String, String> mode_pair in modes)
                foreach (KeyValuePair<String, String> map_pair in maps)
                    // skip Wake Island for ConquestSmall1
                    // skip all non Expansion Pack 1 maps from ConquestSmall1
                    if (!(mode_pair.Value.Equals("csa") &&
                        (map_pair.Value.Equals("wake_island") || !map_pair.Key.Contains("xp1"))))
                        map_interval.Add(mode_pair.Value + "_" + map_pair.Value);

            foreach (String name in map_interval)
            {
                this.integerVariables.Add(name, 0);
                this.integerVarValidators.Add(name, integerValidator);
            }


            this.booleanVarValidators = new Dictionary<string, booleanVariableValidator>();
            this.booleanVarValidators.Add("keep_squads_live", booleanValidator);
            this.booleanVarValidators.Add("keep_squads_round", booleanValidator);
            this.booleanVarValidators.Add("keep_clan_live", booleanValidator);
            this.booleanVarValidators.Add("keep_clans_round", booleanValidator);
            this.booleanVarValidators.Add("use_white_list", booleanValidator);
            this.booleanVarValidators.Add("use_extra_white_lists", booleanValidator);
            this.booleanVarValidators.Add("virtual_mode", booleanValidator);
            this.booleanVarValidators.Add("kick_idle", booleanValidator);
            this.booleanVarValidators.Add("wait_death", booleanValidator);

            this.stringVarValidators = new Dictionary<string, stringVariableValidator>();
            this.stringVarValidators.Add("round_sort", stringValidator);
            this.stringVarValidators.Add("live_sort", stringValidator);
            this.stringVarValidators.Add("console", commandValidator);


            this.floatVariables = new Dictionary<string, float>();
            this.stringListVariables = new Dictionary<string, string>();
            this.stringListVariables.Add("admin_list", @"micovery, admin2, admin3");

            this.stringListVariables.Add("player_kick_wlist", "list of players that should not kicked when idle");
            this.stringListVariables.Add("player_move_wlist", "list of players that should not be moved");
            this.stringListVariables.Add("player_safe_wlist", "list of players that should not be kicked or moved ");
            this.stringListVariables.Add("clan_kick_wlist", "list of clans that should not be kicked when idle");
            this.stringListVariables.Add("clan_move_wlist", "list of clans that should not be moved");
            this.stringListVariables.Add("clan_safe_wlist", "list of clans that should not be kicked or moved");


            this.stringVariables = new Dictionary<string, string>();

            this.stringVariables.Add("round_sort", "spm_desc_round");
            this.stringVariables.Add("live_sort", "time_desc_round");
            this.stringVariables.Add("console", "Type a command here to test");

            this.hiddenVariables = new List<string>();
            this.hiddenVariables.Add("advanced_mode");
            this.hiddenVariables.Add("warn_msg_total_time");
            this.hiddenVariables.Add("warn_msg_countdown_time");
            this.hiddenVariables.Add("warn_msg_interval_time");
            this.hiddenVariables.Add("warn_msg_display_time");
            this.hiddenVariables.Add("quiet_mode");
            this.hiddenVariables.Add("auto_start");
            this.hiddenVariables.Add("virtual_mode");

            /* Grouping settings */

            this.settings_group = new Dictionary<string, List<string>>();


            List<String> whitelist_group = new List<string>();
            whitelist_group.Add("use_extra_white_lists");
            whitelist_group.Add("player_move_wlist");
            whitelist_group.Add("player_kick_wlist");
            whitelist_group.Add("player_safe_wlist");

            whitelist_group.Add("clan_move_wlist");
            whitelist_group.Add("clan_kick_wlist");
            whitelist_group.Add("clan_safe_wlist");

            List<String> round_balancer_group = new List<string>();
            round_balancer_group.Add("keep_clans_round");
            round_balancer_group.Add("keep_squads_round");
            round_balancer_group.Add("round_interval");
            round_balancer_group.Add("round_wait_time");
            round_balancer_group.Add("round_sort");
            round_balancer_group.Add("kick_idle");

            List<String> idle_watch_group = new List<string>();
            idle_watch_group.Add("last_kill_time");
            idle_watch_group.Add("last_death_time");
            idle_watch_group.Add("last_spawn_time");
            idle_watch_group.Add("last_chat_time");
            idle_watch_group.Add("last_score_time");

            List<String> live_balancer_group = new List<string>();
            live_balancer_group.Add("keep_clans_live");
            live_balancer_group.Add("keep_squads_live");
            live_balancer_group.Add("live_sort");
            live_balancer_group.Add("live_interval_time");
            live_balancer_group.Add("warn_say");
            live_balancer_group.Add("wait_death");
            live_balancer_group.Add("wait_death_count");
            live_balancer_group.Add("balance_threshold");
            live_balancer_group.Add("ticket_threshold");

            settings_group.Add("Round Interval", map_interval);
            settings_group.Add("Whitelist", whitelist_group);
            settings_group.Add("Live Balancer", live_balancer_group);
            settings_group.Add("Round Balancer", round_balancer_group);
            settings_group.Add("Idle Watch", idle_watch_group);

            settings_group_order = new Dictionary<string, int>();
            settings_group_order.Add("Settings", 1);
            settings_group_order.Add("Live Balancer", 2);
            settings_group_order.Add("Round Balancer", 3);
            settings_group_order.Add("Whitelist", 4);
            settings_group_order.Add("Idle Watch", 5);
            settings_group_order.Add("Round Interval", 6);

        }



        public void loadSettings()
        {
            attempts = 0;
        }


        private player_sort_method getPlayerSort(string phase)
        {
            string sort_method = getStringVarValue(phase);


            if (sort_method.CompareTo("kdr_asc_round") == 0)
                return player_kdr_asc_round_cmp;
            else if (sort_method.CompareTo("kdr_desc_round") == 0)
                return player_kdr_desc_round_cmp;
            else if (sort_method.CompareTo("score_asc_round") == 0)
                return player_score_asc_round_cmp;
            else if (sort_method.CompareTo("score_desc_round") == 0)
                return player_score_desc_round_cmp;
            else if (sort_method.CompareTo("spm_asc_round") == 0)
                return player_spm_asc_round_cmp;
            else if (sort_method.CompareTo("spm_desc_round") == 0)
                return player_spm_desc_round_cmp;
            else if (sort_method.CompareTo("kpm_asc_round") == 0)
                return player_kpm_asc_round_cmp;
            else if (sort_method.CompareTo("kpm_desc_round") == 0)
                return player_kpm_desc_round_cmp;
            else if (sort_method.CompareTo("time_asc_round") == 0)
                return player_time_asc_round_cmp;
            else if (sort_method.CompareTo("time_desc_round") == 0)
                return player_time_desc_round_cmp;
            else if (sort_method.CompareTo("kdr_asc_online") == 0)
                return player_kdr_asc_online_cmp;
            else if (sort_method.CompareTo("kdr_desc_online") == 0)
                return player_kdr_desc_online_cmp;
            else if (sort_method.CompareTo("kpm_asc_online") == 0)
                return player_kpm_asc_online_cmp;
            else if (sort_method.CompareTo("kpm_desc_online") == 0)
                return player_kpm_desc_online_cmp;
            else if (sort_method.CompareTo("spm_asc_online") == 0)
                return player_spm_asc_online_cmp;
            else if (sort_method.CompareTo("spm_desc_online") == 0)
                return player_spm_desc_online_cmp;
            else if (sort_method.CompareTo("kills_asc_online") == 0)
                return player_kills_asc_online_cmp;
            else if (sort_method.CompareTo("kills_desc_online") == 0)
                return player_kills_desc_online_cmp;
            else if (sort_method.CompareTo("deaths_asc_online") == 0)
                return player_deaths_asc_online_cmp;
            else if (sort_method.CompareTo("deaths_desc_online") == 0)
                return player_deaths_desc_online_cmp;
            else if (sort_method.CompareTo("skill_asc_online") == 0)
                return player_skill_asc_online_cmp;
            else if (sort_method.CompareTo("skill_desc_online") == 0)
                return player_skill_desc_online_cmp;
            else if (sort_method.CompareTo("quits_asc_online") == 0)
                return player_quits_asc_online_cmp;
            else if (sort_method.CompareTo("quits_desc_online") == 0)
                return player_quits_desc_online_cmp;
            else if (sort_method.CompareTo("accuracy_asc_online") == 0)
                return player_accuracy_asc_online_cmp;
            else if (sort_method.CompareTo("accuracy_desc_online") == 0)
                return player_accuracy_desc_online_cmp;
            else if (sort_method.CompareTo("score_asc_online") == 0)
                return player_score_asc_online_cmp;
            else if (sort_method.CompareTo("score_desc_online") == 0)
                return player_score_desc_online_cmp;
            else if (sort_method.CompareTo("rank_asc_online") == 0)
                return player_rank_asc_online_cmp;
            else if (sort_method.CompareTo("rank_desc_online") == 0)
                return player_rank_desc_online_cmp;
            else if (sort_method.CompareTo("random_value") == 0)
                return player_random_value_cmp;

            ConsoleWrite("cannot find player sort method for ^b" + sort_method + "^0 during ^b" + phase + "^n, using default sort");
            return player_spm_asc_round_cmp;
        }


        private squad_sort_method getSquadSort(string phase)
        {
            string sort_method = getStringVarValue(phase);

            if (sort_method.CompareTo("kdr_asc_round") == 0)
                return squad_kdr_asc_round_cmp;
            else if (sort_method.CompareTo("kdr_desc_round") == 0)
                return squad_kdr_desc_round_cmp;
            else if (sort_method.CompareTo("score_asc_round") == 0)
                return squad_score_asc_round_cmp;
            else if (sort_method.CompareTo("score_desc_round") == 0)
                return squad_score_desc_round_cmp;
            else if (sort_method.CompareTo("spm_asc_round") == 0)
                return squad_spm_asc_round_cmp;
            else if (sort_method.CompareTo("spm_desc_round") == 0)
                return squad_spm_desc_round_cmp;
            else if (sort_method.CompareTo("kpm_asc_round") == 0)
                return squad_kpm_asc_round_cmp;
            else if (sort_method.CompareTo("kpm_desc_round") == 0)
                return squad_kpm_desc_round_cmp;
            else if (sort_method.CompareTo("time_asc_round") == 0)
                return squad_time_asc_round_cmp;
            else if (sort_method.CompareTo("time_desc_round") == 0)
                return squad_time_desc_round_cmp;


            else if (sort_method.CompareTo("kdr_asc_online") == 0)
                return squad_kdr_asc_online_cmp;
            else if (sort_method.CompareTo("kdr_desc_online") == 0)
                return squad_kdr_desc_online_cmp;
            else if (sort_method.CompareTo("kpm_asc_online") == 0)
                return squad_kpm_asc_online_cmp;
            else if (sort_method.CompareTo("kpm_desc_online") == 0)
                return squad_kpm_desc_online_cmp;
            else if (sort_method.CompareTo("spm_asc_online") == 0)
                return squad_spm_asc_online_cmp;
            else if (sort_method.CompareTo("spm_desc_online") == 0)
                return squad_spm_desc_online_cmp;
            else if (sort_method.CompareTo("kills_asc_online") == 0)
                return squad_kills_asc_online_cmp;
            else if (sort_method.CompareTo("kills_desc_online") == 0)
                return squad_kills_desc_online_cmp;
            else if (sort_method.CompareTo("deaths_asc_online") == 0)
                return squad_deaths_asc_online_cmp;
            else if (sort_method.CompareTo("deaths_desc_online") == 0)
                return squad_deaths_desc_online_cmp;
            else if (sort_method.CompareTo("skill_asc_online") == 0)
                return squad_skill_asc_online_cmp;
            else if (sort_method.CompareTo("skill_desc_online") == 0)
                return squad_skill_desc_online_cmp;
            else if (sort_method.CompareTo("quits_asc_online") == 0)
                return squad_quits_asc_online_cmp;
            else if (sort_method.CompareTo("quits_desc_online") == 0)
                return squad_quits_desc_online_cmp;
            else if (sort_method.CompareTo("accuracy_asc_online") == 0)
                return squad_accuracy_asc_online_cmp;
            else if (sort_method.CompareTo("accuracy_desc_online") == 0)
                return squad_accuracy_desc_online_cmp;
            else if (sort_method.CompareTo("score_asc_online") == 0)
                return squad_score_asc_online_cmp;
            else if (sort_method.CompareTo("score_desc_online") == 0)
                return squad_score_desc_online_cmp;
            else if (sort_method.CompareTo("rank_asc_online") == 0)
                return squad_rank_asc_online_cmp;
            else if (sort_method.CompareTo("rank_desc_online") == 0)
                return squad_rank_desc_online_cmp;
            else if (sort_method.CompareTo("random_value") == 0)
                return squad_random_value_cmp;

            ConsoleWrite("cannot find squad sort method for ^b" + sort_method + "^0 during ^b" + phase + "^n, using default sort");
            return squad_kpm_desc_round_cmp;
        }


        /* squad comparison methods */

        private int squad_count_asc_cmp(PlayerSquad left, PlayerSquad right)
        {
            int lval = left.getCount();
            int rval = right.getCount();

            return lval.CompareTo(rval);
        }

        private int squad_count_desc_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_count_asc_cmp(left, right) * (-1);
        }


        private int squad_kdr_asc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getRoundKdr();
            double rval = right.getRoundKdr();
            return lval.CompareTo(rval);
        }

        private int squad_kdr_desc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_kdr_asc_round_cmp(left, right) * (-1);
        }

        private int squad_spm_asc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getRoundSpm();
            double rval = right.getRoundSpm();
            return lval.CompareTo(rval);
        }

        private int squad_spm_desc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_spm_asc_round_cmp(left, right) * (-1);
        }


        private int squad_score_asc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getRoundScore();
            double rval = right.getRoundScore();
            return lval.CompareTo(rval);
        }

        private int squad_score_desc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_score_asc_round_cmp(left, right) * (-1);
        }

        private int squad_kpm_asc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getRoundKpm();
            double rval = right.getRoundKpm();
            return lval.CompareTo(rval);
        }

        private int squad_kpm_desc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_kpm_asc_round_cmp(left, right) * (-1);
        }


        private int squad_time_asc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            DateTime lval = left.getRoundTime();
            DateTime rval = right.getRoundTime();
            return lval.CompareTo(rval);
        }

        private int squad_time_desc_round_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_time_asc_round_cmp(left, right) * (-1);
        }


        /* Squad sorting methods based on online stats */

        private int squad_kdr_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineKdr();
            double rval = right.getOnlineKdr();
            return lval.CompareTo(rval);
        }

        private int squad_kdr_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_kdr_asc_online_cmp(left, right) * (-1);
        }

        private int squad_kpm_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineKpm();
            double rval = right.getOnlineKpm();
            return lval.CompareTo(rval);
        }

        private int squad_kpm_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_kpm_asc_online_cmp(left, right) * (-1);
        }


        private int squad_spm_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineSpm();
            double rval = right.getOnlineSpm();
            return lval.CompareTo(rval);
        }

        private int squad_spm_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_spm_asc_online_cmp(left, right) * (-1);
        }

        private int squad_kills_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineKills();
            double rval = right.getOnlineKills();
            return lval.CompareTo(rval);
        }

        private int squad_kills_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_kills_asc_online_cmp(left, right) * (-1);
        }

        private int squad_deaths_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineDeaths();
            double rval = right.getOnlineDeaths();
            return lval.CompareTo(rval);
        }

        private int squad_deaths_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_deaths_asc_online_cmp(left, right) * (-1);
        }


        private int squad_skill_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineSkill();
            double rval = right.getOnlineSkill();
            return lval.CompareTo(rval);
        }

        private int squad_skill_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_skill_asc_online_cmp(left, right) * (-1);
        }


        private int squad_quits_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineQuits();
            double rval = right.getOnlineQuits();
            return lval.CompareTo(rval);
        }

        private int squad_quits_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_quits_asc_online_cmp(left, right) * (-1);
        }


        private int squad_accuracy_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineAccuracy();
            double rval = right.getOnlineAccuracy();
            return lval.CompareTo(rval);
        }

        private int squad_accuracy_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_accuracy_asc_online_cmp(left, right) * (-1);
        }

        private int squad_score_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineScore();
            double rval = right.getOnlineScore();
            return lval.CompareTo(rval);
        }

        private int squad_score_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_score_asc_online_cmp(left, right) * (-1);
        }

        private int squad_rank_asc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            double lval = left.getOnlineRank();
            double rval = right.getOnlineRank();
            return lval.CompareTo(rval);
        }

        private int squad_rank_desc_online_cmp(PlayerSquad left, PlayerSquad right)
        {
            return squad_rank_asc_online_cmp(left, right) * (-1);
        }


        private int squad_random_value_cmp(PlayerSquad left, PlayerSquad right)
        {
            int lval = left.getRandomValue();
            int rval = right.getRandomValue();
            return lval.CompareTo(rval);
        }




        /* player comparison methods */


        private int player_kdr_asc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getRoundKdr();
            double rval = right.getRoundKdr();

            return lval.CompareTo(rval);
        }

        private int player_kdr_desc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_kdr_asc_round_cmp(left, right) * (-1);
        }

        private int player_spm_asc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getRoundSpm();
            double rval = right.getRoundSpm();
            return lval.CompareTo(rval);
        }

        private int player_spm_desc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_spm_asc_round_cmp(left, right) * (-1);
        }


        private int player_score_asc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getRoundScore();
            double rval = right.getRoundScore();
            return lval.CompareTo(rval);
        }

        private int player_score_desc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_score_asc_round_cmp(left, right) * (-1);
        }

        private int player_kpm_asc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getRoundKpm();
            double rval = right.getRoundKpm();
            return lval.CompareTo(rval);
        }

        private int player_kpm_desc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_kpm_asc_round_cmp(left, right) * (-1);
        }


        private int player_time_asc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            DateTime lval = left.getRoundTime();
            DateTime rval = right.getRoundTime();
            return lval.CompareTo(rval);
        }

        private int player_time_desc_round_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_time_asc_round_cmp(left, right) * (-1);
        }


        /* Player sort methodsd based on online stats */
        private int player_kdr_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineKdr();
            double rval = right.getOnlineKdr();
            return lval.CompareTo(rval);
        }

        private int player_kdr_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_kdr_asc_online_cmp(left, right) * (-1);
        }

        private int player_kpm_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineKpm();
            double rval = right.getOnlineKpm();
            return lval.CompareTo(rval);
        }

        private int player_kpm_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_kpm_asc_online_cmp(left, right) * (-1);
        }


        private int player_spm_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineSpm();
            double rval = right.getOnlineSpm();
            return lval.CompareTo(rval);
        }

        private int player_spm_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_spm_asc_online_cmp(left, right) * (-1);
        }

        private int player_kills_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineKills();
            double rval = right.getOnlineKills();
            return lval.CompareTo(rval);
        }

        private int player_kills_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_kills_asc_online_cmp(left, right) * (-1);
        }

        private int player_deaths_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineDeaths();
            double rval = right.getOnlineDeaths();
            return lval.CompareTo(rval);
        }

        private int player_deaths_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_deaths_asc_online_cmp(left, right) * (-1);
        }

        private int player_skill_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineSkill();
            double rval = right.getOnlineSkill();
            return lval.CompareTo(rval);
        }

        private int player_skill_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_skill_asc_online_cmp(left, right) * (-1);
        }


        private int player_quits_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineQuits();
            double rval = right.getOnlineQuits();
            return lval.CompareTo(rval);
        }

        private int player_quits_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_quits_asc_online_cmp(left, right) * (-1);
        }


        private int player_accuracy_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineAccuracy();
            double rval = right.getOnlineAccuracy();
            return lval.CompareTo(rval);
        }

        private int player_accuracy_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_accuracy_asc_online_cmp(left, right) * (-1);
        }

        private int player_score_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineScore();
            double rval = right.getOnlineScore();
            return lval.CompareTo(rval);
        }

        private int player_score_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_score_asc_online_cmp(left, right) * (-1);
        }

        private int player_rank_asc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            double lval = left.getOnlineRank();
            double rval = right.getOnlineRank();
            return lval.CompareTo(rval);
        }

        private int player_rank_desc_online_cmp(PlayerProfile left, PlayerProfile right)
        {
            return player_rank_asc_online_cmp(left, right) * (-1);
        }


        private int player_random_value_cmp(PlayerProfile left, PlayerProfile right)
        {
            int lval = left.getRandomValue();
            int rval = right.getRandomValue();
            return lval.CompareTo(rval);
        }


        public void unloadSettings()
        {
            this.players.Clear();
            removeTask("InsaneBalancer");
            attempts = 0;
        }



        public string GetPluginName()
        {
            return "Insane Balancer";
        }

        public string GetPluginVersion()
        {
            return "0.0.0.6-patch-4";
        }

        public string GetPluginAuthor()
        {
            return "micovery";
        }

        public string GetPluginWebsite()
        {
            return "www.insanegamersasylum.com";
        }


        public string GetPluginDescription()
        {
            return @"
        <h2>Description</h2>
        <p> This is the draft impelementation for a flexible team balancer, which can balance teams by skill, rank, score, kdr and other rules.
            All of it, while doing best effort to maintain squads together, and clans on the same team. 
        </p>

        <h2>Sort Methods</h2>
        <p> A sort method is a rule used for sorting a list of players or squads. The following balancing methods are supported:
        </p>
        <ul>
            
            <li><b>kpm_asc_round</b> , <b>kpm_desc_round</b> <br />
             Sorting based on the soldier round kills per minute 
            </li>
            <li><b>spm_asc_round</b> , <b>spm_desc_round</b> <br />
             Sorting based on the soldier round score per minute
            </li>
            <li><b>kdr_asc_round</b> , <b>kdr_desc_round</b> <br />
             Sorting based on the soldier round kill to death ratio  
            </li>
           
            <li><b>score_asc_round</b> , <b>score_desc_round</b> <br />
             Sorting based on the soldier round score  
            </li>
            <li><b>time_asc_round</b> , <b>time_desc_round</b> <br />
             Sorting based on the time the player joined the server.
            </li>

            <li><b>kdr_asc_online</b> , <b>kdr_desc_online</b> <br />
             Sorting based on the soldier online kill to death ratio
            </li>
            <li><b>kpm_asc_online</b> , <b>kpm_asc_online</b> <br />
             Sorting based on the soldier online kills per minute
            </li>
            <li><b>spm_asc_online</b> , <b>spm_desc_online</b> <br />
             Sorting based on the soldier online score per minute  
            </li> 
            <li><b>kills_asc_online</b> , <b>kills_desc_online</b> <br />
             Sorting based on the soldier online kills  
            </li>
            <li><b>deaths_asc_online</b> , <b>deaths_desc_online</b> <br />
             Sorting based on the soldier online deaths 
            </li>
            <li><b>skill_asc_online</b> , <b>skill_desc_online</b> <br />
             Sorting based on the soldier online skill statistic 
            </li>
            <li><b>quits_asc_online</b> , <b>quits_desc_online</b> <br />
             Sorting based on the soldier online quit percentage
             (a round not completed is counted as a quit)
            </li>
            <li><b>accuracy_asc_online</b> , <b>accuracy_desc_online</b> <br />
             Sorting based on the soldier online accuracy
            </li>
            <li><b>score_asc_online</b> , <b>score_desc_online</b> <br />
             Sorting based on the soldier online total score
            </li>
            </li>
            <li><b>rank_asc_online</b> , <b>rank_desc_online</b> <br />
             Sorting based on the soldier online rank
            </li>
            <li><b>random_value</b><br />
             Sorting is based on random values assigned to the players.<br />
             <br />
             Each player gets assigned a random value. Then, the list of players is sorted/ordered by those random values.<br />
             That way you end up with a random permutation of the players list for shuffling.
            </li>

        </ul>

            All the data for sorting rules ending in <b>_round</b> is obtained from the previous or current round statistics.<br />
            <br />
            All the data for sorting rules ending in <b>_online</b> is obtained from from the battlelog.battlefield.com website.<br />
            <br />
            <br />
            The substrings <b>asc</b>, and <b>desc</b> mean ascending, and descending respectively. This is used for the sorting order.
      
        <h2>Live Balancing Logic</h2> 
                          
        <blockquote>
        Insane Balancer tries to be as un-intrusive as posible while balancing a game that is in progress. 
        If the teams become un-balanced while the game is in progess it will create two pools of players and sort them. 
        (players chosen from the bigger team) One pool for players who are not in any squad, and another pool for squads.
        First it will chose the player at the top of the no-squad pool and move it to the other team until teams are balanced.
        If the no-squad pool becomes empty (and teams are still unbalanced) then squad at the top of the squad pool is moved 
        to the other team if the number of players needed is greater than or equal to the size of the squad. If the number of players
        needed is less than the size of the top squad, a random random player is chosen from the squad and moved to the opposite team 
        until teams are balanced. (players that were on the same squad are kept together)
        </blockquote>

        
        <h2>Round Re-balancing Logic</h2> 
                          
        <blockquote>
        If end of round balancing is enabled, Insane Balancer will completely re-sort teams, even if they are already balanced. 
        The logic for the re-sort is as follows. Create two pools of players and sort them (choosing players from all teams). 
        One pool for players who are not in squads, and another for players who are in squads. Then, move all all players and squads 
        to the neutral team, in order to end up with two empty non-neutral teams. Then, pick the squad at the top of the squad pool, 
        and move it to the losing team. Then pick the next squad on top of the squad pool, and move it to the team with the least players, and so on.
        Once the squad pool is empty, pick the player on top of the no-squad pool, and move it to the team with least players, and so on. 
        If teams are still unbalanced after the no-squad pool is empty, then the live balancing logic is applied.<br />
        <br />
        This is a high-level explanation explanation of the round-balancing logic algorithm. It does not mean that players are actually moved to 
        the neutral team. This is not possible anymore with Battlefield 3. Instead, it's done virtually with 3 arrays of players to represent 
        the three teams: Neutral, US, and RU. When all the calculations are done, and the plugin has determined the target teams and squad for each
        player ... it will then swap players between RU, and US one at a time.<br />
        <br />
        Note that if the server is full, round-balacing logic will not be applied. It's not possible to move players when both teams are full. 
        Also note that, it can happen that the server becomes full while the round-balancing is being applied. When this happens, the admin.movePlayer
        commands may fail. This was not a problem in Battlefield Bad Company 2, because there was a spare/neutral team that players could be moved to
        when balancing a full server.
      
        </blockquote>

        
        <h2>Balanced Team Determination </h2>
         
        <blockquote> 
        Teams are determined to be balanced if the difference in number of players between teams is less than or equal to the <b>balance_threshold</b>.
        The <b>balance_threshold</b> has to be a number greater than 0. If the total number of players in the server is less than or equal to the
        <b>balance_threshold</b>, then the user set threshold is ignored, and a value of 1 is used instead. Technically, no team should ever be bigger 
        than the other by more than the value of <b>balance_threshold</b>.
        </blockquote>

        <h2>Keeping Squads Together </h2>

        <blockquote>
        Insane Balancer is coded to keep squads together by default. However, you can set <b>keep_squads</b> to false and both round-balancing and
        live-balancing logics are changed a bit. What happens, is that all squads in the squad pool are broken, and players put into the no-squad pool, 
        before balancing starts.<br />
        <br />
        Note that for live-balancing, players are not actually moved out of the squad. (it would kill all players if you do that). 
        They are just treated as if they were not in a squad. Also, If <b>keep_squads</b> is enabled, clan-squads will not be broken.
        </blockquote>

        <h2> Keeping Clans On Same Team (requires Battlelog credentials) </h2>   

        <blockquote>                                                                         
        During end-of round re-balancing, if <b>keep_clans</b> is enabled, players with the same clan tag are be removed from their current squad, 
        and put into exclusive squads. These special clan squads are given priority over non-clan squads, so that clan-squads end up in the same team.
        Note that when <b>keep_clans</b> is enabled, teams may end up unbalanced in number, so the live-balancing logic may still need to be applied.<br />
        <br />
        During live-balancing, if <b>keep_clans</b> is enabled, players with clan tags are given priority, as long as there is at least two members of 
        the same clan in the server. When picking players to move to the other team, if a player has a clan tag, the player will be automatically skipped, 
        if the majority of the clan is in the same team (otherwise the player is moved to the other team to join his clan buddies).
        If at the end of live-balancing phase, teams are still unbalanced, then <b>keep_clans</b> is disabled temporarily, and the live-balancer logic is applied again.
        </blockquote>        

        <h2>Settings</h2>
        <ol>
          <li><blockquote><b>balance_threshold</b><br />
                <i>(integer > 0)</i> -  maximum difference in team sizes before teams are considered unbalanced <br />
                Technically, no team will ever be bigger by more than the <b>balance_threshold</b>
                </blockquote> 
          </li> 
          <li><blockquote><b>live_interval_time</b><br />
                <i>(integer > 0)</i> - interval number of seconds at which team balance is checked during game  <br />
                </blockquote> 
          </li> 
          <li><blockquote><b>round_interval</b><br />
                <i>(integer > 0)</i> - interval number number of rounds at which the round balancer is applied  <br />
                For example, if map Atacama has 6 rounds, and the value of <b>round_interval</b> is 2, then the round
                balancer is run at the end of rounds 2, 4, and 6. <br />
                <br />
                This value is used for all maps, unless you provide a per-map interval value. <br />
                Per-map round interval values can be set in the ""Round Interval"" section of the plugin.<br />
                There you will see a list of maps, and game modes supported by BF3. <br />

                <br />
                The following prefixes are used to identify game modes:<br />
                <br />
                <ul>
                   <li><i>cl</i> - conquest large </li>
                   <li><i>cs</i> - conquest small </li>
                   <li><i>csa</i> - conquest small assault (only for BF3 Back to Karkand Maps, except Wake Island) </li>
                   <li><i>rl</i> - rush large </li>
                   <li><i>sr</i> - squad rush </li>
                   <li><i>td</i> - team death-match </li>
                </ul>
                </blockquote> 
          </li>
          <li><blockquote><b>round_wait_time</b><br />
                <i>(integer > 0)</i> - number of seconds to wait after round-over event, before activating the round-end balancer <br />
                </blockquote> 
          </li>
          <li><blockquote><strong>keep_squads_live</strong><br />
                <i>true</i> - squads are preseved during live balancing <br />
                <i>false</i> - squads are intentionally broken up during live balancing
                </blockquote> 
          </li>
          <li><blockquote><strong>keep_squads_round</strong><br />
                <i>true</i> - squads are preseved during end of round balancing <br />
                <i>false</i> - squads are intentionally broken up during end of round balancing
                </blockquote> 
          </li>
          <li><blockquote><strong>keep_clans_round</strong><br />
                <i>true</i> - players with same clan tags are kept on the same team during end of round balancing <br />
                <i>false</i> - clan tags are ignored during end of round balancing
                </blockquote> 
          </li>
          <li><blockquote><strong>keep_clans_live</strong><br />
                <i>true</i> - players with same clan tags are kept on the same team during live balancing <br />
                <i>false</i> - clan tags are ignored during live balancing
                </blockquote> 
          </li>
          <li><blockquote><strong>warn_say</strong><br />
                <i>true</i> - send auto-balancer warning in chat <br />
                <i>false</i> - do not say the auto-balancer warning in chat 
                </blockquote> 
          </li>
          <li><blockquote><strong>balance_round</strong><br />
                <i>true</i> - enables the end of round balancer<br />
                <i>false</i> - disabled the end of round balancer 
                </blockquote> 
          </li>
          <li><blockquote><strong>balance_live</strong><br />
                <i>true</i> - enables the live balancer<br />
                <i>false</i> - disables the live balancer 
                </blockquote> 
          </li>
          <li><blockquote><strong>admin_list</strong><br />
                <i>(string, csv)</i> - list of players who are allow to execute admin commands       
                </blockquote> 
           </li>        
           <li><blockquote><strong>round_sort</strong><br />
                <i>(string)</i> - method used for sorting players and squads during end of round balancing      
                </blockquote> 
           </li>
           <li><blockquote><strong>live_sort</strong><br />
                <i>(string)</i> - method used for sorting players and squads during live balancing      
                </blockquote> 
           </li>

           <li><blockquote><strong>player_safe_wlist</strong><br />
                <i>(string, csv)</i> - list of players that should never be moved or kicked (by the idle kicker)     
                </blockquote> 
           </li>

           <li><blockquote><strong>clan_safe_wlist</strong><br />
                <i>(string, csv)</i> - list of clan (tags) for players that should never be moved or kicked (by the idle kicker)   
                </blockquote> 
           </li>

           <li><blockquote><strong>use_extra_white_lists</strong><br />
                <i>true</i> - enables and shows the extra white-lists<br />
                <i>false</i> - disables and hides the extra white-lists  
                </blockquote> 
           </li>
           <li><blockquote><strong>player_move_wlist</strong><br />
                <i>(string, csv)</i> - list of players that should never be moved    
                </blockquote> 
           </li>

            <li><blockquote><strong>player_kick_wlist</strong><br />
                <i>(string, csv)</i> - list of players that should never be kicked (by the idle kicker)   
                </blockquote> 
           </li>

           <li><blockquote><strong>clan_kick_wlist</strong><br />
                <i>(string, csv)</i> - list of clan (tags) for players that should never be kicked by the idle kicker 
                </blockquote> 
           </li>
           <li><blockquote><strong>clan_move_wlist</strong><br />
                <i>(string, csv)</i> - list of clan (tags) for players that should never be moved    
                </blockquote> 
           </li>

           <li><blockquote><strong>clan_kick_wlist</strong><br />
                <i>(string, csv)</i> - list of clan (tags) for players that should never be kicked   
                </blockquote> 
           </li>
          <li><blockquote><strong>kick_idle</strong><br />
                <i>true</i> - enables the idle kicking feature for the round-end balancer<br />
                <i>false</i> - disables the idle kicking feature for the round-end balancer <br />
               <br />
              If the server is full when the round ends, it is not possible to apply the round-end balancing logic.<br />
              This is where the idle kicker can be useful. If enabled, the idle-kicker will kick one idle player from each team. <br />
              This allows the round-end balancing logic to proceed. <br />
              <br />
              The way it determines if a player is idle, is by keeping track of the time since the last player activity. <br />
              There are five indicators of activity that the plugin keeps track of. These are: <b>chat</b>, <b>kills</b>, <b>deaths</b>, <b>spawn</b>, and <b>score</b>. <br />
              <br />
              For a player to be considered idle, all five activity indicators must be true. If any of the indicators is not true, then the player is not considered idle.<br />
          </blockquote> 
          </li>
           <li><blockquote><strong>last_chat_time</strong><br />
                <i>(integer >= 0)</i> - This is the number of seconds for the idle-kicker chat activity indicator 
                </blockquote> 
           </li>
           <li><blockquote><strong>last_kill_time</strong><br />
                <i>(integer >= 0)</i> - This is the number of seconds for the idle-kicker kill activity indicator 
                </blockquote> 
           </li>
           <li><blockquote><strong>last_death_time</strong><br />
                <i>(integer >= 0)</i> - This is the number of seconds for the idle-kicker death activity indicator 
                </blockquote> 
           </li>
           <li><blockquote><strong>last_spawn_time</strong><br />
                <i>(integer >= 0)</i> - This is the number of seconds for the idle-kicker spawn activity indicator 
                </blockquote> 
           </li>
           <li><blockquote><strong>last_score_time</strong><br />
                <i>(integer >= 0)</i> - This is the number of seconds for the idle-kicker score activity indicator 
                </blockquote> 
           </li>
           <li><blockquote><strong>ticket_threshold</strong><br />
                <i>(integer >= 0)</i> - When the number of tickets in either team goes below this value, live balancer stops working.<br />
                <br />
                For Team-Deathmatch mode, the behavior is reversed. <br />
                For example, if the maximum number of tickets is 100, and the ticket threshold is 20, then<br />
                the live balancer will stop working whenever either team reaches goes over 80 tickets.<br />
                </blockquote> 
           </li>
           <li><blockquote><strong>wait_death</strong><br />
                <i>true</i> - enables the wait death algorithm. Players are not killed by live balancer. They are moved when they die.<br />
                <i>false</i> - disables the wait death algorithm. Players may be killed and and moved while alive, for balancing purposes.<br />
            </blockquote> 
           </li>
           <li><blockquote><strong>wait_death_count</strong><br />
                <i>(integer > 0)</i> - Specifies how big to make the candidates list for moving when <b>wait_death</b> is enabled. <br />
                <br />
                By default this is set to <b>6</b> players, but you may change it as you wish depending on your server size. <br />
                The reason this is needed is because even though only one player may be needed to achieve balance,<br />
                that player may take too long to die, and thus the teams will stay un-balanced much longer. <br />
                <br />
                With the candidate list set to <b>6</b>, it means that 6 players will be chosen from the top of the list<br />
                and marked as possible candidates to be moved.
                </blockquote> 
           </li>
           <li><blockquote><strong>console</strong><br />
                <i>(string)</i> - here you can use to test the in-game commands from within procon. </br>
                For example: ""!show round stats"" will print the player statistic for the current round in the plugin console.     
                </blockquote> 
           </li>
        </ol>

        <h2>Public In-Game Commands</h2>
        <p>
            In-game commands are messages typed into the game chat box, which have special meaning to the plugin. 
            Commands must start with one of the following characters: !,@, or /. This plugin interprets the following commands:
        </p>
        <ul>
           <li><blockquote><strong>!move</strong><br />
               This command can be used by regular players to move themselves to the opposite team as long as teams are balanced.
               </blockquote> 
           </li> 
        </ul>
       <h2> Admin In-Game Commands</h2>
        <p>
            These are the commands that only soldiers in the ""admin_list"" are allowed to execute. Reply messages generated by admin commands
            are sent only to the admin who executed the command.
        </p>
        <ul>
           <li><blockquote><strong>!start check</strong><br />
               This command puts the live balancer in started state, so that it periodically (every <b>live_interval_time</b> seconds) checks the teams for balance. <br />
               When this command is run <b>balance_live</b> is implicitly set to true.
               </blockquote> 
           </li>
           <li><blockquote><strong>!stop check</strong><br />
                This command puts the live balancer in stopped state.
                When this command is run <b>balance_live</b> is implicitly set to false. 
               </blockquote> 
           </li>
           <li><blockquote><strong>!show round stats [player-name]</strong><br />
                This command is used for showing the player statistics for the current round.
                The name of the player is optional. If you do not provide a player name, it will print statistics for all players.
               </blockquote> 
           </li>
           <li><blockquote><strong>!show online stats [player-name]</strong><br />
                This command is used for showing the player online (battlelog) statistics.
                The name of the player is optional. If you do not provide a player name, it will print statistics for all players.
               </blockquote> 
           </li>
           <li><blockquote><strong>!show idle</strong><br />
                Prints a list of the players that the plugin considers idle, based on the indicators of activity.<br />
                 <br />
                 <ul>
                 <li><b>last_chat_time</b><br /></li>
                 <li><b>last_kill_time</b><br /></li>
                 <li><b>last_death_time</b><br /></li>
                 <li><b>last_spawn_time</b><br /></li>
                 <li><b>last_score_time</b><br /></li>
                 </ul>
                 <br />
               </blockquote> 
           </li>
          <li><blockquote><strong>!wlist_info {player-name}</strong><br />
                This command is used for testing/checking what white-lists a player is in, e.g.:
                 <br />
                 <br />
                 !wlist_info micovery
                 <br />
                 player_safe_wlist = False<br />
                 clan_safe_wlist = False<br />
                 player_kick_wlist = <b>True</b><br />
                 player_move_wlist = False<br />
                 clan_kick_wlist = False<br />
                 clan_move_wlist = False<br />
                 <br />
                 <br />
                 In the example above, the player is only in the ""player_kick_wlist"".
                 If the player is not in-gmae, you will not be able to see white-list information.
                
               </blockquote> 
           </li>
           <li><blockquote><strong>!balance live</strong><br />
               This command forces the live balancing logic to be applied whithout any warning period or countdown. 
               </blockquote> 
           </li>
          <li><blockquote><strong>!balance round</strong><br />
               This command forces the round balancing logic to be applied whithout any warning period or countdown. 
               </blockquote> 
           </li>
           <li><blockquote>
                <strong>1. !set {variable} {to|=} {value}</strong><br />
                <strong>2. !set {variable} {value}</strong><br />       
                <strong>3. !set {variable}</strong><br />   
                This command is used for setting the value of this plugin's variables.<br />
                For the 2nd invocation syntax you cannot use ""="" or ""to"" as the variable value. <br />
                For the 3rd invocation syntax the value is assumed to be ""true"".
               </blockquote> 
           </li>
           <li><blockquote>
                <strong>!get {variable} </strong><br />
                This command prints the value of the specified variable.
               </blockquote> 
           </li>
         </ul> 
        ";
        }

        public void OnPluginLoaded(string strHostName, string strPort, string strPRoConVersion)
        {
            ConsoleWrite("plugin loaded");
            this.RegisterEvents("OnPlayerJoin",
                                "OnPlayerKilled",
                                "OnPlayerSpawned",
                                "OnPlayerLeft",
                                "OnGlobalChat",
                                "OnTeamChat",
                                "OnSquadChat",
                                "OnLevelStarted",
                                "OnPunkbusterplayerStatsCmd",
                                "OnServerInfo",
                                "OnPlayerTeamChange",
                                "OnPlayerMovedByAdmin",
                                "OnPlayerKickedByAdmin",
                                "OnPlayerSquadChange",
                                "OnplayersStatsCmd",
                                "OnRoundOver");
        }

        public void OnPluginEnable()
        {


            ConsoleWrite("^b^2Enabled!^0");

            plugin_enabled = true;

            unloadSettings();
            loadSettings();


            addPluginCallTask("InsaneBalancer", "ticks", 0, 1, -1);
            initializeBalancer();
        }



        public void addPluginCallTask(string task, string method, int delay, int interval, int repeat)
        {
            this.ExecuteCommand("procon.protected.tasks.add", task, delay.ToString(), interval.ToString(), repeat.ToString(), "procon.protected.plugins.call", "InsaneBalancer", method);
        }

        public void removeTask(string task)
        {
            this.ExecuteCommand("procon.protected.tasks.remove", task);
        }

        public int getElapsedTime(DateTime now, PluginState state)
        {
            DateTime startTime = getStartTime(state);
            int elapsed = (int)now.Subtract(startTime).TotalSeconds;
            return elapsed;
        }

        public DateTime getStartTime(PluginState state)
        {
            if (state.Equals(PluginState.wait))
                return startWaitTime;
            else if (state.Equals(PluginState.warn))
                return startWarnTime;
            else if (state.Equals(PluginState.check))
                return startCheckTime;
            else if (state.Equals(PluginState.balance))
                return startBalanceTime;
            else if (state.Equals(PluginState.stop))
                return startStopTime;
            else
                ConsoleWrite("^1^bWARNING^0^n: cannot find start time for ^b" + state.ToString() + "^n^0");

            return utc;
        }

        public void setStartTime(PluginState state, DateTime now)
        {
            if (state.Equals(PluginState.wait))
                startWaitTime = now;
            else if (state.Equals(PluginState.warn))
                startWarnTime = now;
            else if (state.Equals(PluginState.check))
                startCheckTime = now;
            else if (state.Equals(PluginState.balance))
                startBalanceTime = now;
            else if (state.Equals(PluginState.stop))
                startStopTime = now;
            else
                ConsoleWrite("^1^bWARNING^0^n: cannot set start time for ^b" + state.ToString() + "^n^0");
        }


        public int getMaxTime(PluginState state)
        {
            if (state.Equals(PluginState.wait))
                return getIntegerVarValue("live_interval_time");
            else if (state.Equals(PluginState.warn))
                return getIntegerVarValue("warn_msg_total_time");
            /*else
                DebugWrite("^1Getting max time for ^b" + state.ToString() + "^n state is not valid", 6);
            */

            return getElapsedTime(utc, PluginState.check);
        }

        public int getRemainingTime(DateTime now, PluginState state)
        {

            int max_time = getMaxTime(state);
            int elapsed = getElapsedTime(now, state);

            int remain = max_time - elapsed;
            return remain;
        }


        public void ExecCommand(params string[] args)
        {
            List<string> list = new List<string>();
            list.Add("procon.protected.send");
            list.AddRange(args);
            this.ExecuteCommand(list.ToArray());
        }



        public void getPlayerList()
        {
            ExecCommand("admin.listPlayers", "all");
            ExecCommand("serverInfo");
            ExecCommand("punkBuster.pb_sv_command", "pb_sv_plist");
        }

        public void getServerInfo()
        {
            ExecCommand("serverInfo");
        }


        public void ticks()
        {
            utc = utc.AddSeconds(1);
            timer(utc);

        }

        public bool isPluginState(PluginState state)
        {
            return pluginState.Equals(state);
        }

        public bool isPluginWaiting()
        {
            return isPluginState(PluginState.wait);
        }

        public bool isPluginBalancing()
        {
            return isPluginState(PluginState.balance);
        }

        public bool isPluginWarning()
        {
            return isPluginState(PluginState.warn);
        }

        public bool isPluginStopped()
        {
            return isPluginState(PluginState.stop);
        }

        public bool isPluginChecking()
        {
            return isPluginState(PluginState.check);
        }

        public void startCheckState(DateTime now)
        {

            try
            {
                if (check_state_phase == 0)
                {
                    pluginState = PluginState.check;
                    setStartTime(pluginState, now.AddSeconds(1));
                    DebugWrite("^b" + pluginState + "^n state started " + getStartTime(pluginState).ToString() + "^n^0", 1);

                    DebugWrite("^b" + PluginState.check.ToString() + "^n state ^bphase-" + check_state_phase + "^n started " + getStartTime(pluginState).ToString() + "^0", 2);
                    DebugWrite("Requesting player list", 2);

                    check_state_phase = 1;
                    getPlayerList();


                    return;
                }
                else if (check_state_phase == 1)
                {

                    DebugWrite("^b" + PluginState.check.ToString() + "^n state ^bphase-" + check_state_phase + "^n started " + now.ToString() + "^0", 2);



                    if (teamsUnbalanced())
                    {
                        DebugWrite("Teams are unbalanced, going to ^b" + PluginState.warn.ToString() + "^n state", 2);
                        startWarnState(now);
                    }
                    else
                    {
                        DebugWrite("Teams are balanced, going to ^b" + PluginState.wait.ToString() + "^n state", 2);
                        restartWaitState(now);
                    }

                    check_state_phase = 0;

                    return;
                }
            }
            catch (Exception e)
            {
                dump_exception(e);
            }
        }

        public void timer(DateTime now)
        {

            if (!getBooleanVarValue("balance_live"))
                return;

            int remain_time = getRemainingTime(now, pluginState);
            int elapsed_time = getElapsedTime(now, pluginState);


            if (isPluginChecking() || isPluginStopped() || isPluginBalancing())
                DebugWrite(pluginState.ToString() + "(" + elapsed_time + ")", 4);
            else
                DebugWrite(pluginState.ToString() + "(" + remain_time + ")", 4);



            if (isPluginStopped())
            {
                if (getBooleanVarValue("auto_start"))
                {
                    DebugWrite("^bauto_start^n is enabled, going to ^b" + PluginState.wait.ToString() + "^n state^0", 2);
                    startWaitSate(now);
                }
            }
            else if (isPluginWaiting())
            {
                if (remain_time <= 0)
                    startCheckState(now);
            }
            else if (isPluginWarning())
            {
                int countdown_time = getIntegerVarValue("warn_msg_countdown_time");
                int display_time = getIntegerVarValue("warn_msg_display_time");
                int interval_time = getIntegerVarValue("warn_msg_interval_time");

                if (teamsBalanced())
                {
                    DebugWrite("Teams are balanced, halting ^b" + PluginState.warn.ToString() + "^n state, and restarting ^b" + PluginState.wait.ToString() + "^n state^0", 4);
                    restartWaitState(now);
                    return;
                }
                else if (remain_time <= 0)
                {
                    balanceLive(now);
                    restartWaitState(now);
                    return;
                }
                else if (remain_time >= 1 && remain_time <= countdown_time)
                {
                    warnCountdown();
                    return;
                }
                else if (isTimeLeft(remain_time, display_time, interval_time, countdown_time))
                {
                    warnAnnounce(display_time);
                    return;
                }
            }
        }


        private bool teamsUnbalanced()
        {
            return !teamsBalanced();
        }

        private bool teamsBalanced()
        {

            //return false;
            /* initialize hash with player count for 16 teams*/
            Dictionary<int, int> player_count = getPlayerCount();
            int total = player_count[1] + player_count[2];

            int difference = Math.Abs(player_count[1] - player_count[2]);
            int balance_threshold = getIntegerVarValue("balance_threshold");

            /* assumer the minimum threshold if user total players is less than user set threshold */
            int threshold = (total <= balance_threshold) ? 1 : balance_threshold;



            if (difference <= balance_threshold)
                return true;

            return false;
        }

        private int sumSquadPlayers(List<PlayerSquad> squads)
        {
            int sum = 0;
            foreach (PlayerSquad squad in squads)
                sum += squad.getCount();
            return sum;
        }

        private void listPlayers()
        {
            DebugWrite("== Listing Players == ", 3);
            listPlayers(getPlayersProfile(""));

        }

        private void listPlayers(List<PlayerProfile> players_list)
        {
            int count = 1;
            foreach (PlayerProfile player in players_list)
            {
                DebugWrite("    " + count + ". ^b" + player + "^n STeam(" + TN(player.getSavedTeamId()) + ").SSquad(" + SQN(player.getSavedSquadId()) + ") ... Team(" + TN(player.getTeamId()) + ").Squad(" + SQN(player.getSquadId()) + ")", 3);
                count++;
            }
        }


        private List<PlayerSquad> getSquadsNotInWhiteList(List<PlayerSquad> squads)
        {
            return getSquadsByWhiteList(squads, false);
        }

        private List<PlayerSquad> getSquadsInWhiteList(List<PlayerSquad> squads)
        {
            return getSquadsByWhiteList(squads, true);
        }

        private List<PlayerSquad> getSquadsByWhiteList(List<PlayerSquad> squads, bool flag)
        {
            List<PlayerSquad> list = new List<PlayerSquad>();

            foreach (PlayerSquad squad in squads)
            {
                bool in_whitelist = false;
                foreach (PlayerProfile player in squad.getMembers())
                    if (isInMoveWhiteList(player))
                    {
                        in_whitelist = true;
                        break;
                    }

                if (flag && in_whitelist)
                    list.Add(squad);
                else if (!flag && !in_whitelist)
                    list.Add(squad);

            }

            return list;
        }

        private int mergePlayerWithTeam(PlayerProfile pp, int toTeamId)
        {

            if (pp.getTeamId() == toTeamId)
                return 0;

            int players_moved = 0;
            int squad_max_sz = 4;
            int nosquadId = 0;

            List<PlayerSquad> squads = getAllSquads(toTeamId);

            /* sort the squads in increasing order of player count */
            squads.Sort(new Comparison<PlayerSquad>(squad_count_asc_cmp));

            DebugWrite("First looking for empty slots in squads for " + pp + " in Team(" + TN(toTeamId) + ")", 3);
            for (int i = 0; i < squads.Count; i++)
            {
                PlayerSquad sorted_squad = squads[i];

                if (sorted_squad.getCount() == squad_max_sz)
                    continue;

                if (movePlayer(pp, sorted_squad.getTeamId(), sorted_squad.getSquadId()))
                {
                    DebugWrite(pp + " moved to Team(" + TN(sorted_squad.getTeamId()) + ").Squad(" + SQN(sorted_squad.getTeamId()) + ")", 3);
                    sorted_squad.addPlayer(pp);
                    players_moved++;
                    break;
                }
            }

            if (players_moved > 0)
                return players_moved;

            DebugWrite("Could not find empty slots in squads for " + pp + " in Team(" + TN(toTeamId) + ")", 3);
            if (movePlayer(pp, toTeamId, nosquadId))
            {
                DebugWrite(pp + " moved to Team(" + TN(toTeamId) + ").Squad(" + SQN(nosquadId) + ")", 3);
                players_moved++;
            }

            return players_moved;
        }

        private int mergeSquadWithPool(PlayerSquad squad, List<PlayerSquad> squads)
        {

            int players_moved = 0;
            if (squad == null)
                return 0;

            int squad_max_sz = 4;

            List<PlayerProfile> squad_players = squad.getMembers();


            /* sort the squads in increasing order of player count */

            squads.Sort(new Comparison<PlayerSquad>(squad_count_asc_cmp));

            for (int i = 0; i < squads.Count; i++)
            {
                PlayerSquad sorted_squad = squads[i];
                if (squad.getTeamId() == sorted_squad.getTeamId() &&
                    squad.getSquadId() == sorted_squad.getSquadId())
                    continue;

                while (sorted_squad.getFreeSlots() > 0 && squad_players.Count > 0)
                {
                    PlayerProfile squad_player = squad_players[0];
                    squad_players.RemoveAt(0);
                    if (movePlayer(squad_player, sorted_squad.getTeamId(), sorted_squad.getSquadId()))
                    {
                        DebugWrite(squad_player + " moved to Team(" + TN(squad_player.getTeamId()) + ").Squad(" + SQN(squad_player.getSquadId()) + ")", 3);
                        sorted_squad.addPlayer(squad_player);
                        players_moved++;
                    }
                }
            }

            return players_moved;
        }

        private int kickOnePlayer(List<PlayerProfile> list)
        {
            foreach (PlayerProfile pp in list)
            {
                if (isInKickWhiteList(pp))
                {
                    DebugWrite("Player " + pp + " is idle, but cannot kick because he in white-list", 3);
                    continue;
                }
                ConsoleWrite("Kicking idle player ^bTeam(" + TN(pp.getTeamId()) + ").Squad(" + SQN(pp.getSquadId()) + ") " + pp + " from server");
                KickPlayerWithMessage(pp, "kicked for inactivity when server was full");
                return 1;
            }

            return 0;
        }



        private Dictionary<int, List<PlayerProfile>> getAllIdle()
        {
            List<PlayerProfile> all = getPlayersProfile("");

            Dictionary<int, List<PlayerProfile>> idle = new Dictionary<int, List<PlayerProfile>>();

            //pre initialize the idle list with 0

            idle.Add(0, new List<PlayerProfile>());
            idle.Add(1, new List<PlayerProfile>());
            idle.Add(2, new List<PlayerProfile>());

            foreach (PlayerProfile pp in all)
            {
                if (!isPlayerIdle(pp))
                    continue;

                if (idle[pp.getTeamId()].Contains(pp))
                    continue;

                idle[pp.getTeamId()].Add(pp);
            }

            return idle;

        }

        private void balanceRound(int winTeamId)
        {
            if (winTeamId == 0)
                winTeamId = 1;

            if (serverInfo == null)
            {
                ConsoleWrite("^1^bERROR^0^n: will not run round-balancer, server size information is not available");
                return;
            }

            if (!getBooleanVarValue("balance_round"))
            {
                ConsoleWrite("Round balancer disbaled, not running");
                return;
            }

            int loseTeamId = getOpposingTeamId(winTeamId);
            int neutralTeamId = 0;
            int team_sz = serverInfo.MaxPlayerCount / 2;

            /* find where the free slot is */
            Dictionary<int, int> pcounts = getPlayerCount();
            int total = pcounts[winTeamId] + pcounts[loseTeamId];


            if (total > serverInfo.MaxPlayerCount)
            {
                ConsoleWrite("^1^bWARNING^n^0: detected that there are ^b" + total + "^n players, but DICE says the server size is ^b" + serverInfo.MaxPlayerCount + "^n ");
                ConsoleWrite("^1^bWARNING^n^0: This makes no sense, DICE be trolling you! ");
                ConsoleWrite("The highest player count I saw this round was ^b" + max_player_count + "^n, I will use that as server size instead (cross fingers)");
                team_sz = max_player_count / 2;
            }


            Dictionary<int, int> fslots = new Dictionary<int, int>();
            fslots.Add(neutralTeamId, 0);
            fslots.Add(winTeamId, team_sz - pcounts[winTeamId]);
            fslots.Add(loseTeamId, team_sz - pcounts[loseTeamId]);

            if (!(fslots[winTeamId] > 0 || fslots[loseTeamId] > 0))
            {
                if (getBooleanVarValue("kick_idle"))
                {
                    Dictionary<int, List<PlayerProfile>> idle = getAllIdle();
                    DebugWrite("^bkick_idle^n is ^bon^n, will try to find idle players to kick on Team(" + TN(winTeamId) + ") or Team(" + TN(loseTeamId) + ")", 3);


                    if (idle[winTeamId].Count == 0 && idle[loseTeamId].Count == 0)
                    {
                        ConsoleWrite("^1^bWARNING^0^n: No free player slots in either team, and no idle players, not balancing");
                        return;
                    }

                    /* kick at least 1 from win team */
                    fslots[winTeamId] += kickOnePlayer(idle[winTeamId]);

                    /* kick at least 1 from lose team */
                    fslots[loseTeamId] += kickOnePlayer(idle[loseTeamId]);

                    if (!(fslots[winTeamId] > 0 || fslots[loseTeamId] > 0))
                    {
                        ConsoleWrite("^1^bWARNING^0^n: Cannot find at least one idle player that is not in white-list, not balancing");
                        return;
                    }
                }

                ConsoleWrite("^1^bWARNING^0^n: No free player slots in either team, not balancing");
                return;
            }

            DebugWrite("Team(" + TN(winTeamId) + ") has " + fslots[winTeamId] + " free slots", 3);
            DebugWrite("Team(" + TN(loseTeamId) + ") has " + fslots[loseTeamId] + " free slots", 3);


            int max_squads = 16;
            int max_squad_size = 4;
            virtual_mode = true;
            DateTime now = utc;
            pluginState = PluginState.balance;
            setStartTime(pluginState, now.AddSeconds(1));
            DebugWrite("^b" + pluginState + "_clans^n state started " + getStartTime(pluginState).ToString() + "^n^0", 1);


            DebugWrite("Saving original teams state", 3);
            List<PlayerProfile> all = getPlayersProfile("");
            all.ForEach(delegate(PlayerProfile pp) { pp.saveTeamSquad(); });
            listPlayers();



            DebugWrite("Building no-squad pool from ^bTeam(" + TN(winTeamId) + ")^n and ^bTeam(" + TN(loseTeamId) + ")^n^0", 3);
            List<PlayerProfile> win_nosquad_pool = getNoSquadPlayers(winTeamId);
            List<PlayerProfile> lose_nosquad_pool = getNoSquadPlayers(loseTeamId);
            List<PlayerProfile> nosquad_pool = new List<PlayerProfile>();
            nosquad_pool.AddRange(win_nosquad_pool);
            nosquad_pool.AddRange(lose_nosquad_pool);

            DebugWrite("No-squad pool has ^b" + nosquad_pool.Count + "^n player/s^0", 2);
            DebugWrite("Building squad pool from ^bTeam(" + TN(winTeamId) + ")^n and ^bTeam(" + TN(loseTeamId) + ")^n^0", 3);

            List<PlayerSquad> win_squad_pool = getNonEmptySquads(winTeamId);
            List<PlayerSquad> lose_squad_pool = getNonEmptySquads(loseTeamId);
            List<PlayerSquad> squad_pool = new List<PlayerSquad>();
            squad_pool.AddRange(win_squad_pool);
            squad_pool.AddRange(lose_squad_pool);



            DebugWrite("Squad pool has ^b" + squad_pool.Count + "^n squads^0", 3);
            listSquads(squad_pool);

            if (!getBooleanVarValue("keep_squads_round"))
            {
                foreach (PlayerSquad squad in squad_pool)
                {
                    DebugWrite("Breaking up ^bTeam(" + TN(squad.getTeamId()) + ").Squad(" + SQN(squad.getSquadId()) + ")^n^0", 3);
                    while (squad.getCount() > 0)
                    {
                        PlayerProfile player = squad.removeRandomPlayer();
                        if (movePlayer(player, player.getTeamId(), 0, true))
                            nosquad_pool.Add(player); /* only add to no-squad list if move succeeded */
                    }
                }
            }

            DebugWrite("Moving no-squad pool to neutral ^bTeam(" + TN(neutralTeamId) + ")^n^0", 3);
            List<PlayerProfile> nosquad_pool_remove = new List<PlayerProfile>();
            foreach (PlayerProfile player in nosquad_pool)
            {
                if (!movePlayer(player, neutralTeamId, 0, true))
                    nosquad_pool_remove.Add(player); /*  move failed, remove him from no squad pool (probably in whitelist) */

            }

            /* remove the players that were marked for removing */
            foreach (PlayerProfile pp in nosquad_pool_remove)
                nosquad_pool.Remove(pp);

            DebugWrite("Moving squad pool to neutral ^bTeam(" + TN(neutralTeamId) + ")^n^0", 3);
            foreach (PlayerSquad squad in squad_pool)
                moveSquad(squad, neutralTeamId, team_sz * 2);


            /* re-build the pools */
            DebugWrite("", 3);
            DebugWrite("Rebuilding no-squad pool from ^bTeam(" + TN(winTeamId) + ")^n and ^bTeam(" + TN(loseTeamId) + ")^n^0", 3);
            nosquad_pool = getNoSquadPlayers(neutralTeamId);
            DebugWrite("No-squad pool has ^b" + nosquad_pool.Count + "^n player/s^0", 3);


            DebugWrite("", 3);
            DebugWrite("Rebuilding squad pool from ^bTeam(" + TN(winTeamId) + ")^n and ^bTeam(" + TN(loseTeamId) + ")^n^0", 3);
            squad_pool = getNonEmptySquads(neutralTeamId);
            DebugWrite("Squad pool has ^b" + squad_pool.Count + "^n squads^0", 2);
            listSquads(squad_pool);


            if (getBooleanVarValue("keep_clans_round"))
            {
                DebugWrite("Keeping clans in same team", 3);

                /* collect statistics about clans */
                DebugWrite("Collecting clan statistics", 3);
                Dictionary<string, int> clan_stats = new Dictionary<string, int>();
                getClanStats(nosquad_pool, clan_stats);
                foreach (PlayerSquad squad in squad_pool)
                    getClanStats(squad.getMembers(), clan_stats);


                List<string> clanlist = new List<string>(clan_stats.Keys);
                DebugWrite("^b" + clanlist.Count + "^n clans in server: [^b" + String.Join("^n], [^b", clanlist.ToArray()) + "^n]", 3);


                int count = 1;
                foreach (KeyValuePair<string, int> pair in clan_stats)
                {
                    DebugWrite("    " + count + ". clan [^b" + pair.Key + "^n] has ^b" + pair.Value + "^n member/s", 3);
                    count++;
                }


                /* for clans with more than two players in game, create a new squad for them, and remove them from their current squads */
                Dictionary<string, List<PlayerSquad>> clan_squads = new Dictionary<string, List<PlayerSquad>>();
                DebugWrite("Creating clan squads from ^bno-squad^n pool", 3);
                getClanSquads(nosquad_pool, clan_squads, clan_stats);
                DebugWrite("Creating clan squads from ^bsquad-spool^n pool", 3);
                foreach (PlayerSquad squad in squad_pool)
                    getClanSquads(squad.getMembers(), clan_squads, clan_stats);

                /* remove the empty squads */
                DebugWrite("Removing empty squads", 3);
                squad_pool.RemoveAll(delegate(PlayerSquad squad) { return squad.getCount() == 0; });
                DebugWrite("squad-pool has now " + squad_pool.Count, 3);


                int new_squads_count = 0;
                List<PlayerSquad> clan_squad_pool = new List<PlayerSquad>();
                foreach (KeyValuePair<string, List<PlayerSquad>> pair in clan_squads)
                {
                    List<PlayerSquad> csquads = pair.Value;
                    DebugWrite(csquads.Count + " squads for clan ^b[" + csquads[0].getMajorityClanTag() + "]^n", 3);
                    DebugWrite("----------------------------------------------------------", 3);
                    listSquads(csquads);

                    clan_squad_pool.AddRange(csquads);
                    new_squads_count += csquads.Count;
                }



                List<int> empty_squad_ids = new List<int>();
                DebugWrite("Total of ^b" + new_squads_count + "^n new clan-squads created", 3);
                if ((max_squads - squad_pool.Count) < new_squads_count)
                {
                    int squads_to_remove_count = 0;
                    int squads_to_ignore_count = 0;
                    int fixed_new_count = 0;

                    int free_squad_slots = (max_squads - squad_pool.Count);
                    int extra = new_squads_count - free_squad_slots;

                    if (extra < squad_pool.Count)
                        squads_to_remove_count = extra;
                    else
                        squads_to_remove_count = squad_pool.Count;


                    squads_to_ignore_count = extra - squads_to_remove_count;

                    fixed_new_count = new_squads_count - squads_to_ignore_count;

                    if (squads_to_ignore_count > 0)
                        DebugWrite("Out of those new clan-squads,  ^b" + squads_to_ignore_count + "^n  will be ignored", 3);

                    DebugWrite("There are already " + squad_pool.Count + "^n non-clan squads in ^bTeam(" + TN(neutralTeamId) + ")^n", 3);
                    DebugWrite("Out of those non-clan squads, " + squads_to_remove_count + " will be removed to make room for " + fixed_new_count + " new clan-squads", 3);


                    List<PlayerSquad> squads_to_remove = new List<PlayerSquad>();

                    /* find squads to remove from the list of squads without players in white-list*/
                    List<PlayerSquad> no_whitelist_squads = getSquadsNotInWhiteList(squad_pool);

                    /* we may have not found enough squads to remove from the non-whitelist squads */
                    int extra_squads_needed = 0;
                    if (no_whitelist_squads.Count < squads_to_remove_count)
                    {
                        extra_squads_needed = squads_to_remove_count - no_whitelist_squads.Count;
                        squads_to_remove_count = no_whitelist_squads.Count;
                        DebugWrite("Only found ^b" + no_whitelist_squads.Count + " non-clan squads without players in white-list, ^b" + squads_to_remove_count + "^n are needed", 3);
                        DebugWrite("Will pick ^b" + extra_squads_needed + "^n extra squads from non-clan pool to remove regardless of white-list", 3);
                    }

                    squads_to_remove.AddRange(no_whitelist_squads.GetRange(0, squads_to_remove_count));
                    if (extra_squads_needed > 0)
                    {
                        List<PlayerSquad> whitelisted_squads = getSquadsInWhiteList(squad_pool);
                        squads_to_remove.AddRange(whitelisted_squads.GetRange(0, extra_squads_needed));
                    }

                    DebugWrite("The following squads will be removed removed from ^bTeam(" + TN(neutralTeamId) + ")", 3);
                    DebugWrite("----------------------------------------------------------------------", 3);
                    listSquads(squads_to_remove);

                    while (squads_to_remove.Count > 0)
                    {
                        PlayerSquad squad_to_remove = squads_to_remove[0];
                        squads_to_remove.RemoveAt(0);
                        empty_squad_ids.Add(squad_to_remove.getSquadId());
                        int squad_sz = squad_to_remove.getCount();

                        DebugWrite("Looking for empty slots in existing squads, for memebers of " + squad_to_remove, 3);

                        squad_sz -= mergeSquadWithPool(squad_to_remove, clan_squad_pool);
                        squad_sz -= mergeSquadWithPool(squad_to_remove, squad_pool);


                        if (squad_sz > 0)
                        {
                            DebugWrite("Did not find empty slots for all members of, " + squad_to_remove, 3);
                            DebugWrite("Will move them to no-squad spool, " + squad_to_remove, 3);
                            while (squad_to_remove.getCount() > 0)
                            {
                                PlayerProfile player = squad_to_remove.removeRandomPlayer();
                                if (movePlayer(player, player.getTeamId(), 0))
                                    nosquad_pool.Add(player);
                            }
                        }

                        /* remove the empty squads */
                        DebugWrite("Removing empty squads", 3);
                        squad_pool.RemoveAll(delegate(PlayerSquad squad) { return squad.getCount() == 0; });
                    }



                }


                DebugWrite("Team(" + TN(neutralTeamId) + ") squad pool before move", 3);
                DebugWrite("=====================================================", 3);
                List<PlayerSquad> temp_pool = getNonEmptySquads(neutralTeamId);
                DebugWrite("Temp pool has ^b" + temp_pool.Count + "^n squads^0", 2);
                temp_pool.Sort(new Comparison<PlayerSquad>(getSquadSort("round_sort")));
                for (int i = 0; i < squad_pool.Count; i++)
                {
                    DebugWrite("      " + i + ". " + temp_pool[i].ToString() + "(" + getSortFieldValueStr(temp_pool[i], "round_sort") + ")", 3);
                }


                /* add clan squads to the squad pool */
                DebugWrite("Moving ^b" + clan_squad_pool.Count + "^n from clan-squad pool to Team(" + TN(neutralTeamId) + ")", 3);
                foreach (PlayerSquad squad in clan_squad_pool)
                    moveSquad(squad, neutralTeamId, team_sz * 2);

            }

            /* re-build the pools */

            DebugWrite("Rebuilding no-squad pool", 3);
            nosquad_pool = getNoSquadPlayers(neutralTeamId);
            DebugWrite("No-squad pool has ^b" + nosquad_pool.Count + "^n player/s^0", 3);
            listPlayers(nosquad_pool);

            DebugWrite("Rebuilding squad pool", 3);
            squad_pool = getNonEmptySquads(neutralTeamId);
            DebugWrite("Squad pool has ^b" + squad_pool.Count + "^n squads^0", 2);


            if (squad_pool.Count > max_squads)
                ConsoleWrite("^1^bWARNING^0^n: There are still more squads than allowed!");

            /* sort the no-squad pool */
            DebugWrite("Sorting the no-squad pool by ^b" + getStringVarValue("round_sort") + "^n^0", 3);
            nosquad_pool.Sort(new Comparison<PlayerProfile>(getPlayerSort("round_sort")));

            for (int i = 0; i < nosquad_pool.Count; i++)
            {
                DebugWrite("      " + i + ". " + nosquad_pool[i] + "(" + getSortFieldValueStr(nosquad_pool[i], "round_sort") + ")", 3);
            }


            /* sort the squad pool */
            DebugWrite("Sorting the squad pool by ^b" + getStringVarValue("round_sort") + "^n^0", 3);
            squad_pool.Sort(new Comparison<PlayerSquad>(getSquadSort("round_sort")));

            for (int i = 0; i < squad_pool.Count; i++)
            {
                DebugWrite("      " + i + ". " + squad_pool[i].ToString() + "(" + getSortFieldValueStr(squad_pool[i], "round_sort") + ")", 3);
            }

            int[] teamCount = new int[3];
            teamCount[neutralTeamId] = sumSquadPlayers(squad_pool) + nosquad_pool.Count;
            teamCount[winTeamId] = getPlayerCount()[winTeamId];
            teamCount[loseTeamId] = getPlayerCount()[loseTeamId];

            DebugWrite("Team counts, ^bTeam(" + TN(neutralTeamId) + ")^n: " + teamCount[neutralTeamId] + ", ^bTeam(" + TN(winTeamId) + ")^n: " + teamCount[winTeamId] + ", ^bTeam(" + TN(loseTeamId) + ")^n: " + teamCount[loseTeamId], 3);

            Dictionary<string, int> clanTeam = new Dictionary<string, int>();

            /* assume the smaller team */
            int smallTeamId = loseTeamId;

            DebugWrite("Moving ^b" + squad_pool.Count + "^n squads from neutral ^bTeam(" + TN(neutralTeamId) + ")^n into ^bTeam(" + TN(winTeamId) + ")^n and ^bTeam(" + TN(loseTeamId) + ")^n^0", 3);

            while (squad_pool.Count > 0)
            {
                /* get the top squad */
                PlayerSquad squad = squad_pool[0];
                squad_pool.RemoveAt(0);

                if (getBooleanVarValue("keep_clans_round"))
                {
                    string tag = squad.getMajorityClanTag();
                    /* if squad has a clan tag, determine where most of his team is already */
                    if (!Regex.Match(tag, @"^\s*$").Success)
                    {
                        if (clanTeam.ContainsKey(tag))
                        {
                            int clan_team_id = clanTeam[tag];
                            DebugWrite("There is already a clan squad for ^b[" + tag + "]^n in ^bTeam(" + TN(clan_team_id) + ")^n^0", 3);
                            smallTeamId = clan_team_id;
                        }
                        else
                        {
                            DebugWrite("First time seeing clan ^b[" + tag + "]^n will assign to ^bTeam(" + TN(smallTeamId) + ")^n^0", 3);
                            clanTeam.Add(tag, smallTeamId);
                        }
                    }
                }

                int squad_sz = squad.getCount();

                /* move top squad to the smaller team */
                DebugWrite("Moving entire " + squad.ToString() + " to ^bTeam(" + TN(smallTeamId) + ")^n^0", 3);
                /* squad size may change if not all players were moved, i.e. someone was in white list */
                squad_sz = moveSquad(squad, smallTeamId, team_sz);


                /* update the team counts */
                teamCount[smallTeamId] += squad_sz;
                teamCount[neutralTeamId] -= squad_sz;

                /* determine the smaller team */
                smallTeamId = getSmallTeamId(winTeamId, teamCount[winTeamId], loseTeamId, teamCount[loseTeamId]);
                DebugWrite("Team counts, ^bTeam(" + TN(neutralTeamId) + ")^n: " + teamCount[neutralTeamId] + ", ^bTeam(" + TN(winTeamId) + ")^n: " + teamCount[winTeamId] + ", ^bTeam(" + TN(loseTeamId) + ")^n: " + teamCount[loseTeamId] + ", ^b Small Team(" + TN(smallTeamId) + ")^n: " + teamCount[smallTeamId], 3);
                DebugWrite("------------------------------------------------------------------------------------------------", 3);
            }

            DebugWrite("^bTeam(" + TN(winTeamId) + ")^n has now ^b" + teamCount[winTeamId] + "^n player/s", 3);
            DebugWrite("^bTeam(" + TN(loseTeamId) + ")^n has now ^b" + teamCount[loseTeamId] + "^n player/s", 3);

            DebugWrite("Moving ^b" + nosquad_pool.Count + "^n player/s from neutral ^bTeam(" + TN(neutralTeamId) + ")^n into ^bTeam(" + TN(winTeamId) + ")^n and ^bTeam(" + TN(loseTeamId) + ")^n^0", 3);

            while (nosquad_pool.Count > 0)
            {
                /* get the top player */
                PlayerProfile player = nosquad_pool[0];
                nosquad_pool.RemoveAt(0);

                /* move the top player to the smaller team */
                DebugWrite("Moving ^b" + player.ToString() + "^n to ^bTeam(^n" + TN(smallTeamId) + ")^n^0", 3);
                int moved = 1;
                if (!movePlayer(player, smallTeamId, 0, true))
                    moved = 0;

                /* update the team counts */
                teamCount[neutralTeamId] -= moved;
                teamCount[smallTeamId] += moved;

                /* determine the smaller team */
                smallTeamId = getSmallTeamId(winTeamId, teamCount[winTeamId], loseTeamId, teamCount[loseTeamId]);
            }


            DebugWrite("^bTeam(" + TN(winTeamId) + ")^n has now ^b" + teamCount[winTeamId] + "^n player/s", 3);
            DebugWrite("^bTeam(" + TN(loseTeamId) + ")^n has now ^b" + teamCount[loseTeamId] + "^n player/s", 3);

            if (teamsUnbalanced())
            {
                DebugWrite("Teams are still unbalanced, applying the live balancing logic", 3);
                balanceLive(utc, true);


            }
            else
                DebugWrite("Teams should now be balanced!", 3);

            DebugWrite("Doing sanity check now", 3);
            DebugWrite("========================", 3);

            List<PlayerProfile> moving_from_win_to_lose = getPlayersMoving(winTeamId, loseTeamId);
            List<PlayerProfile> moving_from_lose_to_win = getPlayersMoving(loseTeamId, winTeamId);



            DebugWrite(moving_from_win_to_lose.Count + " players will be moving from Team(" + TN(winTeamId) + ") to Team(" + TN(loseTeamId) + ")", 3);
            DebugWrite(moving_from_lose_to_win.Count + " players will be moving from Team(" + TN(loseTeamId) + ") to Team(" + TN(winTeamId) + ")", 3);

            int teamWithMoreMoving = 0;
            int players_needing_moving = 0;
            List<PlayerProfile> players_moving = null;

            if (moving_from_win_to_lose.Count > moving_from_lose_to_win.Count)
            {
                teamWithMoreMoving = winTeamId;
                players_moving = moving_from_win_to_lose;
                int slots_needed_in_lose_team = moving_from_win_to_lose.Count - moving_from_lose_to_win.Count;
                DebugWrite(slots_needed_in_lose_team + " free slots are needed in Team(" + TN(loseTeamId) + "), and there are " + fslots[loseTeamId], 3);
                if (slots_needed_in_lose_team > fslots[loseTeamId])
                {
                    DebugWrite("There are only " + fslots[loseTeamId] + " free slots in Team(" + TN(loseTeamId) + ")", 3);
                    players_needing_moving = slots_needed_in_lose_team - fslots[loseTeamId];
                    DebugWrite("Meaning that, " + players_needing_moving + " players will not be able to move from Team(" + TN(winTeamId) + ") to Team(" + TN(loseTeamId) + ")", 3);
                }
            }
            else if (moving_from_lose_to_win.Count > moving_from_win_to_lose.Count)
            {
                teamWithMoreMoving = loseTeamId;
                players_moving = moving_from_lose_to_win;
                int slots_needed_in_win_team = moving_from_lose_to_win.Count - moving_from_win_to_lose.Count;
                DebugWrite(slots_needed_in_win_team + " free slots are needed in Team(" + TN(winTeamId) + "), and there are " + fslots[winTeamId], 3);
                if (slots_needed_in_win_team > fslots[winTeamId])
                {
                    DebugWrite("There are only " + fslots[winTeamId] + " free slots in Team(" + TN(winTeamId) + ")", 3);
                    players_needing_moving = slots_needed_in_win_team - fslots[winTeamId];
                    DebugWrite("Meaning that, " + players_needing_moving + " players will not be able to move from Team(" + TN(loseTeamId) + ") to Team(" + TN(winTeamId) + ")", 3);
                }
            }

            if (players_needing_moving > 0)
            {
                DebugWrite("I have determined that " + players_needing_moving + " players will need to stay in Team(" + TN(teamWithMoreMoving) + ") to remedy this situation", 3);

                while (players_needing_moving > 0 && players_moving.Count > 0)
                {
                    PlayerProfile pp = players_moving[0];
                    players_moving.RemoveAt(0);

                    /* he already stays, leave him alone */
                    if (pp.getSavedTeamId() == pp.getTeamId())
                        continue;

                    /* move him back to where he was */
                    players_needing_moving -= mergePlayerWithTeam(pp, pp.getSavedTeamId());

                }

            }

            DebugWrite("Re-doing sanity check now", 3);
            DebugWrite("========================", 3);

            moving_from_win_to_lose = getPlayersMoving(winTeamId, loseTeamId);
            moving_from_lose_to_win = getPlayersMoving(loseTeamId, winTeamId);

            DebugWrite(moving_from_win_to_lose.Count + " players will be moving from Team(" + TN(winTeamId) + ") to Team(" + TN(loseTeamId) + ")", 3);
            DebugWrite(moving_from_lose_to_win.Count + " players will be moving from Team(" + TN(loseTeamId) + ") to Team(" + TN(winTeamId) + ")", 3);

            virtual_mode = false;

            /* swap players that are moving across teams only, and put them in no-squad */
            fixTeams(winTeamId, pcounts, fslots);

            /* fix the squads for players */
            sleep = true;
            fixSquads();
            sleep = false;
        }



        private List<PlayerProfile> getPlayersMoving(int fromTeamId, int toTeamId)
        {
            List<PlayerProfile> list = new List<PlayerProfile>();
            List<PlayerProfile> player_list = getPlayersProfile("");

            foreach (PlayerProfile pp in player_list)
                if (pp.getSavedTeamId() == fromTeamId &&
                    pp.getTeamId() == toTeamId)
                    list.Add(pp);
            return list;
        }




        private void balanceLive(DateTime now)
        {
            balanceLive(now, false);
        }


        private void delayedLiveBalance(DateTime now, bool force)
        {

            /* save the original team and squad for each player */
            List<PlayerProfile> players = getPlayersProfile("");
            players.ForEach(delegate(PlayerProfile pp)
            {
                if (pp.getDelayedTeamId() > 0)
                    DebugWrite("Un-flagging ^b" + pp + "^n for delayed move", 3);
                pp.resetDelayedTeamSquad();
                pp.saveTeamSquad();
            });


            live_balancer = true;

            /* for delayed live balance we want to do it all virtually, and only move players after they respawn */
            bool original_value = virtual_mode;
            if (!original_value)
                virtual_mode = true;



            balanceLive(now, force, true);
            virtual_mode = original_value;

            ConsoleWrite("virtual live-balance done, proceeding now to flag players that will need moving");

            /* save the delayed team, and squad for each player */
            players = getPlayersProfile("");
            players.ForEach(delegate(PlayerProfile pp)
            {
                if (pp.getSavedTeamId() != pp.getTeamId())
                {
                    /* save the delayed team and squad */
                    pp.saveDelayedTeamSquad();

                    /* reset the original team and squad */
                    pp.setTeamId(pp.getSavedTeamId());
                    pp.setSquadId(pp.getSavedSquadId());

                    if (pp.isAlive())
                    {
                        DebugWrite("Player ^b" + pp + "^n is alive, flagged for delayed move from ^bTeam(" + TN(pp.getSavedTeamId()) + ").Squad(" + SQN(pp.getSavedSquadId()) + ")^n to ^bDTeam(" + TN(pp.getDelayedTeamId()) + ").DSquad(" + SQN(pp.getDelayedSquadId()) + ")^n", 3);
                    }
                    else
                    {
                        DebugWrite("Player ^b" + pp + "^n " + playerstate2stringED(pp.state) + ", flagged for immediate move from ^bTeam(" + TN(pp.getSavedTeamId()) + ").Squad(" + SQN(pp.getSavedSquadId()) + ")^n to ^bDTeam(" + TN(pp.getDelayedTeamId()) + ").DSquad(" + SQN(pp.getDelayedSquadId()) + ")^n", 3);
                        /* skip balance check, we alreay know teams are not balanced */
                        enforceImmediateMove(pp);
                    }
                }
                pp.resetSavedTeamSquad();
            });


            live_balancer = false;
        }

        private void balanceLive(DateTime now, bool force)
        {
            if (getBooleanVarValue("wait_death"))
            {
                ConsoleWrite("^bwait_death^n is ^bon^n, will do live-balance in virtual mode");
                delayedLiveBalance(now, force);
            }
            else
            {
                ConsoleWrite("^bwait_death^n is ^boff^n, will do live-balance on the fly");
                balanceLive(now, force, false);
            }
        }

        private bool checkTicketThreshold()
        {
            List<TeamScore> scores = null;
            lock (info_mutex)
            {
                if (serverInfo == null)
                    return false;

                scores = serverInfo.TeamScores; ;
            }

            // check that the scores are available
            if (scores == null || scores.Count == 0)
            {
                ConsoleWrite("^1^bERROR^0^n: team scores not available");
                return false;
            }

            int min_tickets = getIntegerVarValue("ticket_threshold");
            int min_score = int.MaxValue;
            int min_team = int.MaxValue;

            
            // find the team with the least tickets
            foreach (TeamScore score in scores)
            {
                if (score == null)
                {
                    ConsoleWrite("^1^bERROR^0^n: team score not available");
                    return false;
                }

                if (score.Score > min_score)
                    continue;
                min_score = score.Score;
                min_team = score.TeamID;
            }

            if (min_score <= min_tickets)
            {
                ConsoleWrite("Not running live balancer, Team(" + TN(min_team) + ") has only " + min_score + " ticket" + ((min_score > 1) ? "s" : "") + " left to lose");
                return true;
            }
     
       
            return false;
        }

        private void balanceLive(DateTime now, bool force, bool delayed)
        {
            try
            {
                if (wait_state && !force)
                {
                    ConsoleWrite("Cannot run live balancer, round-over wait state is active");
                    return;
                }

                if (round_balancer && !force)
                {
                    ConsoleWrite("Cannot run live balancer, round-balancing is active");
                    return;
                }

                // do not balance if there min ticket have been reached
                if (!force && checkTicketThreshold())
                    return;


                if (balanceTeams(now) > 0 && getBooleanVarValue("keep_clans_live"))
                {
                    ConsoleWrite("Re-running live balancer, with ^bkeep_clans_live^n disabled");
                    setBooleanVarValue("keep_clans_live", false);
                    if (balanceTeams(now) > 0 && getBooleanVarValue("use_white_list"))
                    {
                        ConsoleWrite("Re-running live balancer, with ^buse_white_list^n disabled");
                        setBooleanVarValue("use_white_list", false);
                        balanceTeams(now);
                        setBooleanVarValue("use_white_list", true);
                    }
                    setBooleanVarValue("keep_clans_live", true);
                }
            }
            catch (Exception e)
            {
                dump_exception(e);
            }
        }




        private void fixSquads()
        {

        }

        private void fixTeams(int winTeamId, Dictionary<int, int> pcounts, Dictionary<int, int> fslots)
        {
            int loseTeamId = getOpposingTeamId(winTeamId);
            int neutralTeamId = 0;
            int noSquadId = 0;

            DebugWrite("====================================================================", 3);
            DebugWrite("Virtual calculations done, now proceeding to actually move players!", 3);
            List<PlayerProfile> players_list = getPlayersProfile("");

            /* save everyone's target team and squad */
            players_list.ForEach(delegate(PlayerProfile p) { p.saveTargetTeamSquad(); });

            /* get players moving across teams */
            List<PlayerProfile> moving_from_win_to_lose = getPlayersMoving(winTeamId, loseTeamId);
            List<PlayerProfile> moving_from_lose_to_win = getPlayersMoving(loseTeamId, winTeamId);

            DebugWrite(moving_from_win_to_lose.Count + " players will be moving from Team(" + TN(winTeamId) + ") to Team(" + TN(loseTeamId) + ")", 3);
            DebugWrite(moving_from_lose_to_win.Count + " players will be moving from Team(" + TN(loseTeamId) + ") to Team(" + TN(winTeamId) + ")", 3);

            /* get aggreagte moving across teams */
            List<PlayerProfile> all_moving = new List<PlayerProfile>();
            all_moving.AddRange(moving_from_win_to_lose);
            all_moving.AddRange(moving_from_lose_to_win);
            DebugWrite("Total of " + all_moving.Count + " players will be moving across teams", 3);

            /* get players staying in their own team */
            List<PlayerProfile> staying_in_win = getPlayersMoving(winTeamId, winTeamId);
            List<PlayerProfile> staying_in_lose = getPlayersMoving(loseTeamId, loseTeamId);

            DebugWrite(staying_in_win.Count + " players will be staying in Team(" + TN(winTeamId) + ")", 3);
            DebugWrite(staying_in_lose.Count + " players will be staying in Team(" + TN(loseTeamId) + ")", 3);

            /* aggregate players staying in their own team */
            List<PlayerProfile> all_staying = new List<PlayerProfile>();
            all_staying.AddRange(staying_in_win);
            all_staying.AddRange(staying_in_lose);
            DebugWrite("Total of " + all_staying.Count + " players will be staying in their own team", 3);



            List<PlayerProfile> all_players = new List<PlayerProfile>();
            all_players.AddRange(all_moving);
            all_players.AddRange(all_staying);
            DebugWrite("Total of " + all_players.Count + " in server", 3);


            DebugWrite("Swapping players that move across Team(" + TN(winTeamId) + ") and Team(" + TN(loseTeamId) + ")", 3);
            DebugWrite("Team(" + TN(winTeamId) + ") has " + fslots[winTeamId] + " free slots", 3);
            DebugWrite("Team(" + TN(loseTeamId) + ") has " + fslots[loseTeamId] + " free slots", 3);


            int teamWithMostFreeSlots = 0;
            int teamWithLeastFreeSlots = 0;
            List<PlayerProfile> teamToChooseFromFirst = null;
            List<PlayerProfile> teamToChooseFromSecond = null;

            if (fslots[winTeamId] > fslots[loseTeamId])
            {
                teamWithMostFreeSlots = winTeamId;
                teamWithLeastFreeSlots = loseTeamId;
                teamToChooseFromFirst = moving_from_lose_to_win;
                teamToChooseFromSecond = moving_from_win_to_lose;

                DebugWrite("Team(" + TN(teamWithMostFreeSlots) + ") has the most free slots", 3);

            }
            else if (fslots[loseTeamId] > fslots[winTeamId])
            {
                teamWithMostFreeSlots = loseTeamId;
                teamWithLeastFreeSlots = winTeamId;
                teamToChooseFromFirst = moving_from_win_to_lose;
                teamToChooseFromSecond = moving_from_lose_to_win;

                DebugWrite("Team(" + TN(teamWithMostFreeSlots) + ") has the most free slots", 3);
            }
            else
            {
                teamWithMostFreeSlots = winTeamId;
                teamWithLeastFreeSlots = loseTeamId;
                teamToChooseFromFirst = moving_from_lose_to_win;
                teamToChooseFromSecond = moving_from_win_to_lose;
                DebugWrite("Both Team(" + TN(winTeamId) + ") and Team(" + TN(loseTeamId) + " have same number of free slots", 3);
            }


            DebugWrite("I will start the swapping from Team(" + TN(teamWithLeastFreeSlots) + ") to Team(" + TN(teamWithMostFreeSlots) + ")", 3);

            while (teamToChooseFromFirst.Count > 0 && teamToChooseFromSecond.Count > 0)
            {
                PlayerProfile player_first = teamToChooseFromFirst[0];
                PlayerProfile player_second = teamToChooseFromSecond[0];
                teamToChooseFromFirst.RemoveAt(0);
                teamToChooseFromSecond.RemoveAt(0);

                DebugWrite("Swapping" + player_first + " from Team(" + TN(player_first.getSavedTeamId()) + ") with " + player_second + " from Team(" + TN(player_second.getSavedTeamId()) + ")", 3);

                PlayerProfile pp = player_first;
                DebugWrite("Moving " + pp + " to from STeam(" + TN(pp.getSavedTeamId()) + ").SSquad(" + SQN(pp.getSavedSquadId()) + ") to TTeam(" + TN(pp.getTargetTeamId()) + ").TSquad(" + SQN(noSquadId) + ")", 3);
                movePlayer(player_first, player_first.getTargetTeamId(), noSquadId, true, true);

                pp = player_second;
                DebugWrite("Moving " + pp + " to from STeam(" + TN(pp.getSavedTeamId()) + ").SSquad(" + SQN(pp.getSavedSquadId()) + ") to TTeam(" + TN(pp.getTargetTeamId()) + ").TSquad(" + SQN(noSquadId) + ")", 3);
                movePlayer(player_second, player_second.getTargetTeamId(), noSquadId, true, true);

                DebugWrite("-------------------------------------------------------------------------------------------------------------------------", 3);
            }

            int teamIdWithRemainingPlayer = 0;
            List<PlayerProfile> teamWithRemainingPlayer = new List<PlayerProfile>();

            if (teamToChooseFromFirst.Count > teamToChooseFromSecond.Count)
            {
                teamIdWithRemainingPlayer = teamWithMostFreeSlots;
                teamWithRemainingPlayer = teamToChooseFromFirst;
            }
            else if (teamToChooseFromSecond.Count > teamToChooseFromFirst.Count)
            {
                teamIdWithRemainingPlayer = teamWithLeastFreeSlots;
                teamWithRemainingPlayer = teamToChooseFromSecond;
            }

            if (teamWithRemainingPlayer.Count > 0)
            {
                DebugWrite("There are still " + teamWithRemainingPlayer.Count + " in Team(" + TN(teamIdWithRemainingPlayer) + ") to be moved", 3);

                foreach (PlayerProfile pp in teamWithRemainingPlayer)
                {
                    DebugWrite("Moving " + pp + " to from STeam(" + TN(pp.getSavedTeamId()) + ").SSquad(" + SQN(pp.getSavedSquadId()) + ") to TTeam(" + TN(pp.getTargetTeamId()) + ").TSquad(" + SQN(noSquadId) + ")", 3);
                    movePlayer(pp, pp.getTargetTeamId(), noSquadId, true, true);
                    DebugWrite("-------------------------------------------------------------------------------------------------------------------------", 3);
                }
            }




            List<PlayerProfile> move = new List<PlayerProfile>();
            /* move all players to no-squad, unless they are already in the no-squad, or they are already in their final position */

            DebugWrite("Fixing squad positions, will only move players that are switching squads", 3);

            DebugWrite("First putting players in Squad(" + SQN(noSquadId) + "), using " + all_staying.Count + " players that did not move across teams", 3);
            foreach (PlayerProfile pp in all_staying)
            {
                /* skip players that are already in squad 0 anyways */
                if (pp.getSavedSquadId() == 0)
                    continue;

                /* skip players that are already in their final position */
                if (pp.getSavedSquadId() == pp.getTargetSquadId())
                    continue;

                DebugWrite("Moving " + pp + " to from STeam(" + TN(pp.getSavedTeamId()) + ").SSquad(" + SQN(pp.getSavedSquadId()) + ") to TTeam(" + TN(pp.getTargetTeamId()) + ").TSquad(" + SQN(noSquadId) + ")", 3);
                if (pp.getTargetSquadId() == 0)
                {
                    /* after this move, this player end in its final position, no need to move him anymore*/
                    movePlayer(pp, pp.getTargetTeamId(), noSquadId, true, true);
                    continue;
                }

                movePlayer(pp, pp.getTargetTeamId(), noSquadId, true, true);
                move.Add(pp);
            }

            move.AddRange(all_moving);

            /* now put all players in their destination squad */
            DebugWrite("Now putting players in their final squads, using a set of " + move.Count + " players", 3);
            foreach (PlayerProfile pp in move)
            {
                /* skip players that are already in their final position */
                if (pp.getSavedSquadId() == pp.getTargetSquadId())
                    continue;

                DebugWrite("Moving " + pp + " to from STeam(" + TN(pp.getTeamId()) + ").SSquad(" + SQN(pp.getSquadId()) + ") to TTeam(" + TN(pp.getTargetTeamId()) + ").TSquad(" + SQN(pp.getTargetSquadId()) + ")", 3);
                movePlayer(pp, pp.getTargetTeamId(), pp.getTargetSquadId(), true, true);
            }
        }

        private void getClanSquads(List<PlayerProfile> members, Dictionary<string, List<PlayerSquad>> clan_squads, Dictionary<string, int> clan_stats)
        {
            int neutralTeamId = 0;
            int noSquadId = 0;
            int max_squad_size = 4;
            int max_squads = 16;

            members.RemoveAll(delegate(PlayerProfile player)
            {
                /* if player is not in a clan, ignore it*/
                if (!player.isInClan())
                    return false;



                string tag = player.getClanTag();

                /* if less than two players for the clan, ignore it */

                if (clan_stats[tag] < 2)
                    return false;

                /* count total number of squads */
                int total = 0;
                foreach (KeyValuePair<string, List<PlayerSquad>> pair in clan_squads)
                    total += pair.Value.Count;

                if (!clan_squads.ContainsKey(tag))
                    clan_squads[tag] = new List<PlayerSquad>();

                List<PlayerSquad> squads = clan_squads[tag];

                /* if there is no squads, of they are all full, add a new at the end */
                if (squads.Count == 0 || squads[squads.Count - 1].getCount() == max_squad_size)
                {
                    if (total >= max_squads)
                    {
                        if (squads.Count == 0)
                            clan_squads.Remove(tag);

                        DebugWrite(player + " ignored, max squads count reached", 3);
                        return false;
                    }
                    squads.Add(new PlayerSquad(neutralTeamId, getNextFreeClanSquadId(clan_squads)));
                }

                /* add player to the last squad in the clan */
                if (movePlayer(player, neutralTeamId, squads[squads.Count - 1].getSquadId(), true))
                    squads[squads.Count - 1].addPlayer(player);

                return true;
            });
        }

        private int getNextFreeClanSquadId(Dictionary<string, List<PlayerSquad>> clan_squads)
        {
            int count = 1;
            foreach (KeyValuePair<string, List<PlayerSquad>> pair in clan_squads)
                foreach (PlayerSquad squad in pair.Value)
                    count++;

            return 16 + count;
        }

        private void getClanStats(List<PlayerProfile> members, Dictionary<string, int> stats)
        {
            foreach (PlayerProfile player in members)
            {
                if (!player.isInClan())
                    continue;

                string tag = player.getClanTag();

                if (!stats.ContainsKey(tag))
                    stats[tag] = 0;

                stats[tag]++;
            }
        }

        private void getClanStatsByTeam(List<PlayerProfile> members, Dictionary<string, int>[] stats)
        {
            for (int i = 0; i < members.Count; i++)
            {
                PlayerProfile player = members[i];
                if (!player.isInClan())
                    continue;

                string tag = player.getClanTag();
                int teamId = player.getTeamId();
                int oppositeTeamId = getOpposingTeamId(teamId);

                if (!stats[teamId].ContainsKey(tag))
                    stats[teamId].Add(tag, 0);

                if (!stats[oppositeTeamId].ContainsKey(tag))
                    stats[oppositeTeamId].Add(tag, 0);


                stats[teamId][tag]++;
            }
        }


        private int getSmallTeamId(int team1Id, int team1Count, int team2Id, int team2Count)
        {
            return (team1Count < team2Count) ? team1Id : team2Id;
        }

        private int getBigTeamId(int team1Id, int team1Count, int team2Id, int team2Count)
        {
            return (team1Count > team2Count) ? team1Id : team2Id;
        }

        private int getOpposingTeamId(int teamId)
        {
            return (teamId == 0) ? teamId : (teamId == 1) ? 2 : 1;
        }





        private int balanceTeams(DateTime now)
        {


            pluginState = PluginState.balance;
            setStartTime(pluginState, now.AddSeconds(1));
            DebugWrite("^b" + pluginState + "_live^n state started " + getStartTime(pluginState).ToString() + "^n^0", 1);

            bool keep_clans = getBooleanVarValue("keep_clans_live");
            Dictionary<int, int> player_count = getPlayerCount();

            if (player_count[1] == player_count[2])
                return 0;

            int team_sz = serverInfo.MaxPlayerCount / 2;
            int neutral_team = 0;
            int bigger_team = (player_count[1] > player_count[2]) ? 1 : 2;
            int smaller_team = (player_count[1] > player_count[2]) ? 2 : 1;
            int total = player_count[1] + player_count[2];
            int difference = Math.Abs(player_count[1] - player_count[2]);
            int needed = difference / 2;




            DebugWrite("Total of ^b" + total + "^n player/s in server^0", 3);
            for (int i = 1; i < 3; i++)
            {
                DebugWrite("^bTeam(" + TN(i) + ")^n has ^b" + player_count[i] + "^n player/s^0", 3);
            }

            DebugWrite("Teams differ by ^b" + difference + "^n player/s,  ^b" + needed + "^n player/s are needed on ^bTeam(" + TN(smaller_team) + ")^n^0", 3);

            int candidates = needed;
            if (getBooleanVarValue("wait_death"))
            {
                candidates = getIntegerVarValue("wait_death_count");

                int tsz = player_count[smaller_team];

                // check that the candidate list does not exceed the team size
                if (candidates > tsz)
                {
                    DebugWrite("cannot use candidate list size of ^b" + candidates + "^n, Team(" + TN(smaller_team) + ") has only ^b" + tsz + "^n player" + ((tsz > 1) ? "s" : ""), 3);
                    candidates = tsz;
                }
                
                // check that the candidates list size is bigger than 
                if (candidates > needed)
                {
                    needed = candidates;
                    DebugWrite("^bwait_death^n is on, will flag " + needed + " candidate" + ((needed > 1) ? "s" : "") + " for moving", 3);
                }
            }


            DebugWrite("Building no-squad pool from ^bTeam(" + TN(bigger_team) + ")^n^0", 3);
            List<PlayerProfile> nosquad_pool = getNoSquadPlayers(bigger_team);
            DebugWrite("No-squad pool has ^b" + nosquad_pool.Count + "^n player/s^0", 3);

            DebugWrite("Building squad pool from ^bTeam(" + TN(bigger_team) + ")^n^0", 3);
            List<PlayerSquad> squad_pool = getNonEmptySquads(bigger_team);
            DebugWrite("Squad pool has ^b" + squad_pool.Count + "^n squads^0", 3);


            Dictionary<string, int>[] clan_stats = new Dictionary<string, int>[3];
            clan_stats[smaller_team] = new Dictionary<string, int>();
            clan_stats[bigger_team] = new Dictionary<string, int>();
            clan_stats[neutral_team] = new Dictionary<string, int>();

            if (keep_clans)
            {
                DebugWrite("Keeping clans in same team", 3);

                List<PlayerProfile> players_list = getPlayersProfile("");
                /* collect statistics about clans */
                DebugWrite("Collecting clan statistics", 3);
                getClanStatsByTeam(players_list, clan_stats);


                List<string> clanlist = new List<string>();
                clanlist.AddRange(clan_stats[1].Keys);

                DebugWrite("^b" + clanlist.Count + "^n clans in server: [^b" + String.Join("^n], [^b", clanlist.ToArray()) + "^n]", 3);
            }

            if (!getBooleanVarValue("keep_squads_live"))
            {
                DebugWrite("^bkeep_squads_live^n is off, moving players to no-squad pool before balancing", 3);
                foreach (PlayerSquad squad in squad_pool)
                    foreach (PlayerProfile player in squad.getMembers())
                        nosquad_pool.Add(player);

                squad_pool.Clear();
            }

            /* sort the no-squad pool */
            DebugWrite("Sorting the no-squad pool by ^b" + getStringVarValue("live_sort") + "^n^0", 3);
            nosquad_pool.Sort(new Comparison<PlayerProfile>(getPlayerSort("live_sort")));

            for (int i = 0; i < nosquad_pool.Count; i++)
                DebugWrite("      " + i + ". " + nosquad_pool[i] + "(" + getSortFieldValueStr(nosquad_pool[i], "live_sort") + ")", 3);


            DebugWrite("Moving ^b" + needed + "^n players from sorted no-squad pool to ^bTeam(" + TN(smaller_team) + ")^n^0", 3);
            while (needed > 0 && nosquad_pool.Count > 0)
            {
                PlayerProfile player = nosquad_pool[0];
                nosquad_pool.RemoveAt(0);
                string tag = player.getClanTag();

                /* if keeping clans together, and there are more than two players in the clan in the sever */
                if (keep_clans && shouldSkipClanPlayer(player, smaller_team, bigger_team, clan_stats))
                    continue;

                DebugWrite("Moving ^b" + player.ToString() + "^n to ^bTeam(^n" + TN(smaller_team) + ")^n^0", 3);
                if (movePlayer(player, smaller_team, 0, true))
                    needed--;
            }

            /* if teams are balanced, we are done */
            if (needed == 0)
            {
                DebugWrite("Teams should now be balanced!", 3);
                return needed;
            }

            /* teams are not balanced, proceed on squad balancing */

            DebugWrite("Teams are still unbalanced, " + needed + " more player/s needed", 3);

            /* sort the squad pool */
            DebugWrite("Sorting the squad pool by ^b" + getStringVarValue("live_sort") + "^n^0", 3);
            squad_pool.Sort(new Comparison<PlayerSquad>(getSquadSort("live_sort")));

            for (int i = 0; i < squad_pool.Count; i++)
            {
                DebugWrite("      " + i + ". " + squad_pool[i].ToString() + "(" + getSortFieldValueStr(squad_pool[i], "live_sort") + ")", 3);
            }

            DebugWrite("Moving squads from sorted squad pool to ^bTeam(" + TN(smaller_team) + ")^n^0", 3);
            while (needed > 0 && squad_pool.Count > 0)
            {
                PlayerSquad squad = squad_pool[0];
                squad_pool.RemoveAt(0);


                int squad_sz = squad.getCount();
                string squad_uid = squad.ToString();
                string smaller_team_uid = "^bTeam(" + TN(smaller_team) + ")^n";

                DebugWrite("^b" + needed + "^n players are needed on " + smaller_team_uid + "^0", 3);
                DebugWrite(squad_uid + " has ^b" + squad_sz + "^n player/s^0", 2);

                if (needed >= squad_sz)
                {
                    if (keep_clans && shouldSkipClanSquad(squad, smaller_team, bigger_team, clan_stats))
                        continue;


                    /* we can move the entrie squad */
                    DebugWrite("Moving entire " + squad_uid + " to " + smaller_team_uid + "^0", 3);
                    squad_sz = moveSquad(squad, smaller_team, team_sz);
                    needed -= squad_sz;
                }
                else
                {
                    /* we have to break up a squad */
                    PlayerSquad temp_squad = new PlayerSquad(squad.getTeamId(), squad.getSquadId());

                    DebugWrite("Breaking up " + squad_uid + " to get ^b" + needed + "^n player/s^0", 3);
                    DebugWrite("But, first I will sort the members of " + squad_uid, 3);
                    squad.sortMembers(getPlayerSort("live_sort"));
                    for (int i = 0; i < squad.getCount(); i++)
                        DebugWrite("      " + i + ". " + squad.getMembers()[i] + "(" + getSortFieldValueStr(squad.getMembers()[i], "live_sort") + ")", 3);

                    /* get as many players as needed */
                    while (needed > 0 && squad.getCount() > 0)
                    {
                        PlayerProfile player = squad.getMembers()[0];
                        squad.dropPlayer(player);

                        if (keep_clans && shouldSkipClanPlayer(player, smaller_team, bigger_team, clan_stats))
                            continue;

                        if (isInMoveWhiteList(player))
                            continue;

                        temp_squad.addPlayer(player);
                        DebugWrite("Player " + player + " selected to move to " + smaller_team_uid + "^0", 3);
                        needed--;
                    }

                    /* move the temporary squad */
                    moveSquad(temp_squad, smaller_team, team_sz);
                }
            }


            if (needed == 0)
                DebugWrite("Teams should now be balanced!", 3);
            else
                DebugWrite("Teams are still ubalanced!", 3);

            return needed;
        }

        private bool shouldSkipClanSquad(PlayerSquad squad, int smaller_team, int bigger_team, Dictionary<string, int>[] clan_stats)
        {
            int squad_sz = squad.getCount();
            string tag = squad.getMajorityClanTag();

            if (tag.Length > 0 && (clan_stats[bigger_team][tag] + clan_stats[smaller_team][tag]) > 1)
            {
                if (clan_stats[bigger_team][tag] >= clan_stats[smaller_team][tag])
                {
                    DebugWrite("Skipping clan-squad " + squad.ToString() + " because majority of clan is in same team", 3);
                    return true;
                }

                /* update clan stats */
                clan_stats[bigger_team][tag] -= squad_sz;
                clan_stats[smaller_team][tag] += squad_sz;
            }

            return false;
        }

        private bool shouldSkipClanPlayer(PlayerProfile player, int smaller_team, int bigger_team, Dictionary<string, int>[] clan_stats)
        {
            string tag = player.getClanTag();

            if (!clan_stats[bigger_team].ContainsKey(tag))
                clan_stats[bigger_team].Add(tag, 0);

            if (!clan_stats[smaller_team].ContainsKey(tag))
                clan_stats[smaller_team].Add(tag, 0);

            if (player.isInClan() && (clan_stats[bigger_team][tag] + clan_stats[smaller_team][tag]) > 1)
            {
                /* if the majority of the players in the clan are in this team, skip this player */
                if (clan_stats[bigger_team][tag] >= clan_stats[smaller_team][tag])
                {
                    DebugWrite("Skipping clan-player ^b" + player + "^n because majority of clan is in his team", 3);
                    return true;
                }

                /* update the clan stats */
                clan_stats[bigger_team][tag]--;
                clan_stats[smaller_team][tag]++;
            }
            return false;
        }

        private string getSortFieldValueStr(PlayerProfile player, string phase)
        {
            string sort_method = getStringVarValue(phase);

            if (sort_method.CompareTo("kdr_asc_round") == 0 || sort_method.CompareTo("kdr_desc_round") == 0)
                return "kdr_round: " + Math.Round(player.getRoundKdr(), 2);
            else if (sort_method.CompareTo("score_asc_round") == 0 || sort_method.CompareTo("score_desc_round") == 0)
                return "score_round: " + Math.Round(player.getRoundScore(), 2);
            else if (sort_method.CompareTo("spm_asc_round") == 0 || sort_method.CompareTo("spm_desc_round") == 0)
                return "spm_round: " + Math.Round(player.getRoundSpm(), 2);
            else if (sort_method.CompareTo("kpm_asc_round") == 0 || sort_method.CompareTo("kpm_desc_round") == 0)
                return "kpm_round: " + Math.Round(player.getRoundKpm(), 2);
            else if (sort_method.CompareTo("time_asc_round") == 0 || sort_method.CompareTo("time_desc_round") == 0)
                return "time_round: " + player.getRoundTime();
            else if (sort_method.CompareTo("random_value") == 0 || sort_method.CompareTo("random_value") == 0)
                return "random_value: " + player.getRandomValue();
            else if (sort_method.CompareTo("kdr_asc_online") == 0 || sort_method.CompareTo("kdr_desc_online") == 0)
                return "kdr_online: " + Math.Round(player.getOnlineKdr(), 2);
            else if (sort_method.CompareTo("kpm_asc_online") == 0 || sort_method.CompareTo("kpm_desc_online") == 0)
                return "kpm_online: " + Math.Round(player.getOnlineKpm(), 2);
            else if (sort_method.CompareTo("spm_asc_online") == 0 || sort_method.CompareTo("spm_desc_online") == 0)
                return "spm_online: " + Math.Round(player.getOnlineSpm(), 2);
            else if (sort_method.CompareTo("kills_asc_online") == 0 || sort_method.CompareTo("kills_desc_online") == 0)
                return "kills_online: " + player.getOnlineKills();
            else if (sort_method.CompareTo("deaths_asc_online") == 0 || sort_method.CompareTo("deaths_desc_online") == 0)
                return "deaths_online: " + player.getOnlineDeaths();
            else if (sort_method.CompareTo("skill_asc_online") == 0 || sort_method.CompareTo("skill_desc_online") == 0)
                return "skill_online: " + Math.Round(player.getOnlineSkill(), 2);
            else if (sort_method.CompareTo("quits_asc_online") == 0 || sort_method.CompareTo("quits_desc_online") == 0)
                return "quits_online: " + Math.Round(player.getOnlineQuits(), 2);
            else if (sort_method.CompareTo("accuracy_asc_online") == 0 || sort_method.CompareTo("accuracy_desc_online") == 0)
                return "accuracy_online: " + Math.Round(player.getOnlineAccuracy(), 2);
            else if (sort_method.CompareTo("score_asc_online") == 0 || sort_method.CompareTo("score_desc_online") == 0)
                return "score_online: " + player.getOnlineScore();
            else if (sort_method.CompareTo("rank_asc_online") == 0 || sort_method.CompareTo("rank_desc_online") == 0)
                return "rank_online: " + player.getOnlineRank();

            ConsoleWrite("^1^bWARNING^0^n: cannot find player sort method for ^b" + sort_method + "^0");
            return "";
        }

        private string getSortFieldValueStr(PlayerSquad squad, string phase)
        {
            string sort_method = getStringVarValue(phase);

            if (sort_method.CompareTo("kdr_asc_round") == 0 || sort_method.CompareTo("kdr_desc_round") == 0)
                return "kdr_round: " + Math.Round(squad.getRoundKdr(), 2);
            else if (sort_method.CompareTo("score_asc_round") == 0 || sort_method.CompareTo("score_desc_round") == 0)
                return "score_round: " + Math.Round(squad.getRoundScore(), 2);
            else if (sort_method.CompareTo("spm_asc_round") == 0 || sort_method.CompareTo("spm_desc_round") == 0)
                return "spm_round: " + Math.Round(squad.getRoundSpm(), 2);
            else if (sort_method.CompareTo("kpm_asc_round") == 0 || sort_method.CompareTo("kpm_desc_round") == 0)
                return "kpm_round: " + Math.Round(squad.getRoundKpm(), 2);
            else if (sort_method.CompareTo("time_asc_round") == 0 || sort_method.CompareTo("time_desc_round") == 0)
                return "time_round: " + squad.getRoundTime();
            else if (sort_method.CompareTo("random_value") == 0 || sort_method.CompareTo("random_value") == 0)
                return "random_value: " + squad.getRandomValue();
            else if (sort_method.CompareTo("kdr_asc_online") == 0 || sort_method.CompareTo("kdr_desc_online") == 0)
                return "kdr_online: " + Math.Round(squad.getOnlineKdr(), 2);
            else if (sort_method.CompareTo("kpm_asc_online") == 0 || sort_method.CompareTo("kpm_desc_online") == 0)
                return "kpm_online: " + Math.Round(squad.getOnlineKpm(), 2);
            else if (sort_method.CompareTo("spm_asc_online") == 0 || sort_method.CompareTo("spm_desc_online") == 0)
                return "spm_online: " + Math.Round(squad.getOnlineSpm(), 2);
            else if (sort_method.CompareTo("kills_asc_online") == 0 || sort_method.CompareTo("kills_desc_online") == 0)
                return "kills_online: " + Math.Round(squad.getOnlineKills(), 2);
            else if (sort_method.CompareTo("deaths_asc_online") == 0 || sort_method.CompareTo("deaths_desc_online") == 0)
                return "deaths_online: " + Math.Round(squad.getOnlineDeaths(), 2);
            else if (sort_method.CompareTo("skill_asc_online") == 0 || sort_method.CompareTo("skill_desc_online") == 0)
                return "skill_online: " + Math.Round(squad.getOnlineSkill(), 2);
            else if (sort_method.CompareTo("quits_asc_online") == 0 || sort_method.CompareTo("quits_desc_online") == 0)
                return "quits_online: " + Math.Round(squad.getOnlineQuits(), 2);
            else if (sort_method.CompareTo("accuracy_asc_online") == 0 || sort_method.CompareTo("accuracy_desc_online") == 0)
                return "accuracy_online: " + Math.Round(squad.getOnlineAccuracy(), 2);
            else if (sort_method.CompareTo("score_asc_online") == 0 || sort_method.CompareTo("score_desc_online") == 0)
                return "score_online: " + Math.Round(squad.getOnlineScore(), 2);
            else if (sort_method.CompareTo("rank_asc_online") == 0 || sort_method.CompareTo("rank_desc_online") == 0)
                return "rank_online: " + Math.Round(squad.getOnlineRank(), 2);

            ConsoleWrite("^1^bWARNING^0^n: cannot find squad sort method for ^b" + sort_method + "^0");
            return "";
        }

        private bool movePlayer(PlayerProfile player, int teamId, int squadId, bool force)
        {
            return movePlayer(player, teamId, squadId, force, false);
        }

        private bool movePlayer(PlayerProfile player, int teamId, int squadId)
        {
            return movePlayer(player, teamId, squadId, false, false);
        }

        private bool isInMoveWhiteList(PlayerProfile player)
        {
            bool result = isPlayerInWhiteList(player, "player_safe_wlist") || isPlayerInWhiteList(player, "clan_safe_wlist");

            if (getBooleanVarValue("use_extra_white_lists"))
                result |= isPlayerInWhiteList(player, "player_move_wlist") || isPlayerInWhiteList(player, "clan_move_wlist");

            return result;
        }

        private bool isInKickWhiteList(PlayerProfile player)
        {
            bool result = isPlayerInWhiteList(player, "player_safe_wlist") || isPlayerInWhiteList(player, "clan_safe_wlist");

            if (getBooleanVarValue("use_extra_white_lists"))
                result |= isPlayerInWhiteList(player, "player_kick_wlist") || isPlayerInWhiteList(player, "clan_kick_wlist");

            return result;
        }


        private bool isPlayerInWhiteList(PlayerProfile player, String list_name)
        {
            if (!getBooleanVarValue("use_white_list"))
                return false;

            if (!getPluginVars().Contains(list_name))
            {
                ConsoleWrite("^1^bWARNING: ^n^0 unknown white list ^b" + list_name + "^n");
                return false;
            }

            List<String> whitelist = getStringListVarValue(list_name);
            if (whitelist.Count == 0)
                return false;

            String field = "";
            if (Regex.Match(list_name, @"clan").Success)
                field = player.getClanTag();
            else if (Regex.Match(list_name, @"player").Success)
                field = player.name;
            else
            {
                ConsoleWrite("^1^bWARNING:^0^n white list ^b" + list_name + "^n does not contain 'player' or 'clan' sub-string");
                return false;
            }

            if (Regex.Match(field, @"^\s*$").Success)
                return false;

            return whitelist.Contains(field);
        }


        private bool movePlayer(PlayerProfile player, int teamId, int squadId, bool force, bool ignore_white_list)
        {
            if (player == null)
                return false;

            if (!force && player.getTeamId() == teamId && player.getSquadId() == squadId)
            {
                ConsoleWrite("^1^bWARNING^0^n: not moving ^b" + player + "^n to same Team(" + TN(teamId) + ").Squad(" + SQN(squadId) + ")");
                return false;
            }
            else if (!ignore_white_list && isInMoveWhiteList(player))
            {
                ConsoleWrite("^b" + player.ToString() + "^n in white-list, will not move to Team(" + TN(teamId) + ").Squad(" + SQN(squadId) + ")");
                return false;
            }


            /* firt move player to the no-squad, to guarantee a spot (unless he is already goin to the no-squad, or stays in same team) */
            if ((squadId != 0 || player.getTeamId() != teamId) && !(virtual_mode || getBooleanVarValue("virtual_mode")))
            {
                if (sleep)
                    Thread.Sleep(100);
                ExecCommand("admin.movePlayer", player.name, teamId.ToString(), "0", "true");
            }

            /* in virtual mode, don't actually do the move */
            if (!(virtual_mode || getBooleanVarValue("virtual_mode")))
            {
                if (sleep)
                    Thread.Sleep(100);
                ExecCommand("admin.movePlayer", player.name, teamId.ToString(), squadId.ToString(), "true");
            }
            player.setTeamId(teamId);
            player.setSquadId(squadId);
            return true;
        }


        /* best effort to move an entire squad into another team withouth breaking up */

        private int moveSquad(PlayerSquad squad, int teamId, int team_sz)
        {
            int players_moved = 0;
            if (squad == null)
                return 0;

            /* first move all players to the opposite team without squad (to guarantee a spot)*/
            int squadId = 0;
            int noSquadId = 0;


            List<PlayerProfile> squad_players = squad.getMembers();

            /* find a squad on teamId with enough space */
            List<PlayerSquad> squads = getAllSquads(teamId);


            /* find first empty squad */

            foreach (PlayerSquad sq in squads)
            {
                if (sq.getCount() == 0)
                {
                    DebugWrite("Found empty squad " + sq + ", for " + squad, 3);
                    while (squad.getCount() > 0)
                    {
                        PlayerProfile pp = squad.removeRandomPlayer();
                        DebugWrite("Moving ^b" + pp + "^n to Team(" + TN(teamId) + ").Squad(" + SQN(sq.getSquadId()) + ")", 3);
                        if (movePlayer(pp, teamId, sq.getSquadId()))
                            players_moved++;

                    }
                    break;
                }
            }

            if (squad.getCount() == 0)
                return players_moved;

            ConsoleWrite("^1^bWARNING^0^n: Could not find an empty squad on ^bTeam(" + TN(teamId) + ")^n for " + squad.ToString());
            ConsoleWrite("Looking now for squads that are not full^n");

            /* sort the squads in increasing order of player count */

            squads.Sort(new Comparison<PlayerSquad>(squad_count_asc_cmp));

            for (int i = 0; i < squads.Count; i++)
            {
                PlayerSquad sorted_squad = squads[i];
                if (sorted_squad.getSquadId() > 8)
                    continue;

                if (sorted_squad.getFreeSlots() > 0 && squad_players.Count > 0)
                    DebugWrite("Found " + sorted_squad.getFreeSlots() + " free slots on " + sorted_squad, 3);

                while (sorted_squad.getFreeSlots() > 0 && squad_players.Count > 0)
                {
                    PlayerProfile squad_player = squad_players[0];
                    squad_players.RemoveAt(0);
                    DebugWrite("Moving ^b" + squad_player + "^n to Team(" + TN(teamId) + ").Squad(" + SQN(sorted_squad.getSquadId()) + ")", 3);
                    if (movePlayer(squad_player, teamId, sorted_squad.getSquadId()))
                        players_moved++;
                }
            }

            foreach (PlayerProfile pp in squad_players)
            {
                ConsoleWrite("^1^bWARNING^0^n: could not find squad on ^bTeam(" + TN(teamId) + ")^n for ^b" + pp + "^n^0");
                DebugWrite("Moving ^b" + pp + "^n to Team(" + TN(teamId) + ").Squad(" + SQN(noSquadId) + ")", 3);
                if (movePlayer(pp, teamId, noSquadId))
                    players_moved++;
            }

            return players_moved;

        }

        private List<PlayerProfile> removePlayers(List<PlayerProfile> player_list, int max_size)
        {
            if (players == null || players.Count == 0)
            {
                ConsoleWrite("^1^bWARNING^0^n: cannot make a squad without any players");
                return null;
            }

            List<PlayerProfile> removed = new List<PlayerProfile>();
            while (player_list.Count > 0 && removed.Count <= max_size)
            {
                removed.Add(player_list[0]);
                player_list.RemoveAt(0);
            }

            return removed;
        }


        public Dictionary<int, int> getPlayerCount()
        {

            /* initialize hash with player count for 16 teams*/
            Dictionary<int, int> player_count = new Dictionary<int, int>();
            for (int i = 0; i < 16; i++)
                player_count[i] = 0;

            List<PlayerProfile> player_list = getPlayersProfile("");

            foreach (PlayerProfile player in player_list)
                player_count[player.getTeamId()]++;


            return player_count;
        }



        private List<PlayerProfile> getNoSquadPlayers(int teamId)
        {
            Dictionary<int, PlayerSquad> squads = getSquads(teamId);
            /* return the members of the no-squad */
            return squads[0].getMembers();
        }


        private List<PlayerSquad> getAllSquads(int teamId)
        {
            Dictionary<int, PlayerSquad> squads = getSquads(teamId);

            /* remove the no-squad */
            squads.Remove(0);

            List<PlayerSquad> list = new List<PlayerSquad>();
            foreach (KeyValuePair<int, PlayerSquad> pair in squads)
                list.Add(pair.Value);

            return list;
        }



        private List<PlayerSquad> getNonEmptySquads(int teamId)
        {
            Dictionary<int, PlayerSquad> squads = getSquads(teamId);

            /* remove the no-squad */
            squads.Remove(0);

            /* get only the non-empty squads */
            List<PlayerSquad> list = new List<PlayerSquad>();
            foreach (KeyValuePair<int, PlayerSquad> pair in squads)
                if (pair.Value.getCount() > 0)
                    list.Add(pair.Value);

            return list;
        }

        private void listSquad(PlayerSquad sq)
        {
            List<PlayerProfile> members = sq.getMembers();

            DebugWrite("Team(^b" + TN(sq.getTeamId()) + "^n).Squad(^b" + SQN(sq.getSquadId()) + "^n): " + sq.getCount() + " players", 3);
            int count = 1;
            foreach (PlayerProfile pp in members)
                DebugWrite("    " + count++ + ".  ^b" + pp + "^n", 3);
        }

        private void listSquads(List<PlayerSquad> sqs)
        {
            foreach (PlayerSquad sq in sqs)
                listSquad(sq);
        }

        private Dictionary<int, PlayerSquad> getSquads(int teamId)
        {
            int num_squads = 8;
            if (teamId == 0)
                num_squads = 16;

            List<PlayerProfile> player_list = getPlayersProfile("");

            Dictionary<int, PlayerSquad> squads = new Dictionary<int, PlayerSquad>();
            for (int i = 0; i <= num_squads; i++)
                squads[i] = new PlayerSquad(teamId, i);

            foreach (PlayerProfile player in player_list)
            {
                if (player.getTeamId() == teamId && squads.ContainsKey(player.getSquadId()))
                    squads[player.getSquadId()].addPlayer(player);
            }

            return squads;
        }

        private bool isTimeLeft(int remain_time, int msg_display_time, int msg_interval_time, int countdown_time)
        {
            return (remain_time % msg_interval_time) == 0 && (remain_time - msg_display_time) >= countdown_time;
        }

        public bool isServerEmpty()
        {
            return players.Count == 0;
        }

        public void forwardTicks(int count)
        {
            utc = utc.AddSeconds(count);
        }


        public void ConsoleWrite(string msg)
        {
            string prefix = "[^b" + GetPluginName() + "^n] ";
            this.ExecuteCommand("procon.protected.pluginconsole.write", prefix + msg);
        }

        public void DebugWrite(string msg, int level)
        {
            if (getIntegerVarValue("debug_level") >= level)
                ConsoleWrite(msg);
        }

        public void OnPluginDisable()
        {
            
            plugin_enabled = false;

            unloadSettings();

            ConsoleWrite("signaling stats fetching thread to stop");
            wake_handle.Set();
            scratch_handle.Set();
            ConsoleWrite("^b^1Disabled =(^0");

        }

        public String getPluginVariableGroup(String name)
        {
            foreach (KeyValuePair<String, List<String>> group_pair in settings_group)
                if (group_pair.Value.Contains(name))
                    return group_pair.Key;

            return "Settings";
        }

        public int getGroupOrder(String name)
        {
            //if (settings_group_order.ContainsKey(name))
            //    return settings_group_order[name];


            Dictionary<int, String> reverse = new Dictionary<int, string>();
            foreach (KeyValuePair<String, int> pair in settings_group_order)
                reverse.Add(pair.Value, pair.Key);

            int offset = 0;
            for (int i = 0; i <= reverse.Count; i++)
                if (!reverse.ContainsKey(i))
                    continue;
                else
                {
                    if (shouldSkipGroup(reverse[i]))
                        continue;
                    offset++;
                    if (name.Equals(reverse[i]))
                        return offset;
                }


            return offset;
        }

        public bool shouldSkipGroup(String name)
        {

            if ((name.Equals("Round Balancer") || name.Equals("Round Interval")) && !getBooleanVarValue("balance_round"))
                return true;

            if (name.Equals("Live Balancer") && !getBooleanVarValue("balance_live"))
                return true;

            if (name.Equals("Whitelist") && !getBooleanVarValue("use_white_list"))
                return true;

            return false;

        }

        public bool shouldSkipVariable(String name)
        {

            if (Regex.Match(name, @"^(?:player|clan)_(?:move|kick)_wlist").Success && !getBooleanVarValue("use_extra_white_lists"))
                return true;

            if (name.Equals("wait_death_count") && !getBooleanVarValue("wait_death"))
                return true;

            return false;

        }

        public List<CPluginVariable> GetDisplayPluginVariables()
        {
            List<CPluginVariable> lstReturn = new List<CPluginVariable>();



            //lstReturn.Add(new CPluginVariable("Settings|Refresh", typeof(string), "| edit this field to refresh settings |"));

            List<string> vars = getPluginVars(true);





            foreach (string var_name in vars)
            {
                String group_name = getPluginVariableGroup(var_name);
                String var_type = "string";
                String var_value = getPluginVarValue(var_name);
                int group_order = getGroupOrder(group_name);

                if (shouldSkipGroup(group_name))
                    continue;

                if (shouldSkipVariable(var_name))
                    continue;

                if (var_name.Equals("live_sort") || var_name.Equals("round_sort"))
                    var_type = "enum." + var_name + "(" + String.Join("|", getAllowedSorts().ToArray()) + ")";
                else if (var_name.Equals("pass"))
                    var_value = Regex.Replace(getPluginVarValue(var_name), @".", "*");

                lstReturn.Add(new CPluginVariable(group_order + ". " + group_name + "|" + var_name, var_type, var_value));
            }

            return lstReturn;
        }

        //Lists all of the plugin variables.
        public List<CPluginVariable> GetPluginVariables()
        {
            List<CPluginVariable> lstReturn = new List<CPluginVariable>();

            List<string> vars = getPluginVars();
            foreach (string var in vars)
                lstReturn.Add(new CPluginVariable(var, typeof(string), getPluginVarValue(var)));

            return lstReturn;
        }


        public void SetPluginVariable(string var, string val)
        {
            //ConsoleWrite("setting " + var + " to " + val);
            if (var.ToLower().Contains("refresh"))
                return;





            setPluginVarValue(var, val);


        }


        public void OnPlayerJoin(string strSoldierName)
        {
            battleLogConnect();
        }

        public void OnPlayerKilled(Kill killInfo)
        {

            if (killInfo == null)
                return;

            CPlayerInfo killer = killInfo.Killer;
            CPlayerInfo victim = killInfo.Victim;

            PlayerProfile vp = getPlayerProfile(victim.SoldierName);
            PlayerProfile kp = getPlayerProfile(killer.SoldierName);

            if (vp != null)
            {
                vp.state = PlayerState.dead;
                vp.updateInfo(victim);
                vp.updateLastDeath();

                if (vp.getDelayedTeamId() > 0)
                {
                    DebugWrite("Player " + vp + " has died, he was flagged to be moved to ^bDTeam(" + TN(vp.getDelayedTeamId()) + ").DSquad(" + SQN(vp.getDelayedSquadId()) + ")^n", 3);
                    /* do not skip the balance check, this is delayed move, teams may already be balanced */
                    enforceDelayedMove(vp);
                }
            }

            if (kp != null)
            {
                kp.updateInfo(killer);
                kp.updateLastKill();
            }
        }

        private void enforceDelayedMove(PlayerProfile vp)
        {

            int dtid = vp.getDelayedTeamId();
            int dsid = vp.getDelayedSquadId();

            vp.resetDelayedTeamSquad();

            /* if player is already in the delayed team, ignore him */
            if (dtid == vp.getTeamId())
            {
                DebugWrite("Player " + vp + " is already in to ^bDTeam(" + TN(dtid) + ")^n, will skip", 3);
                return;
            }


            /* if teams are already balanced, ignore this player */
            DebugWrite("I will now re-check if teams are balanced", 3);
            if (teamsBalanced())
            {
                DebugWrite("Teams are balanced, will not move player " + vp, 3);
                return;
            }

            DebugWrite("Moving player " + vp + " from ^bTeam(" + TN(vp.getTeamId()) + ").Squad(" + SQN(vp.getSquadId()) + ")^n to ^bDTeam(" + TN(dtid) + ").DSquad(" + SQN(dsid) + ")^n", 3);
            movePlayer(vp, dtid, dsid);

        }

        private void enforceImmediateMove(PlayerProfile vp)
        {

            int dtid = vp.getDelayedTeamId();
            int dsid = vp.getDelayedSquadId();

            vp.resetDelayedTeamSquad();

            DebugWrite("Moving player " + vp + " from ^bTeam(" + TN(vp.getSavedTeamId()) + ").Squad(" + SQN(vp.getSavedSquadId()) + ")^n to ^bDTeam(" + TN(dtid) + ").DSquad(" + SQN(dsid) + ")^n", 3);
            movePlayer(vp, dtid, dsid);

        }




        public void battleLogConnect()
        {
            /*
            if (!blog.isReady())
            {
                if (attempts < 2)
                {
                    ConsoleWrite("Attempting to connect to battlelog.battlefield.com");
                    attempts++;

                    blog.connect();

                }
            }*/
        }


        public void OnPlayerLeft(string strSoldierName)
        {
            PlayerProfile player = getPlayerProfile(strSoldierName);
            if (player != null)
                player.state = PlayerState.left;

            if (this.players.ContainsKey(strSoldierName))
                this.players.Remove(strSoldierName);

        }

        public virtual void OnPlayerKickedByAdmin(string soldierName, string reason)
        {
            PlayerProfile player = getPlayerProfile(soldierName);
            if (player != null)
            {
                player.state = PlayerState.kicked;
                this.players.Remove(player.name);
            }
        }

        public virtual void OnPlayerMovedByAdmin(string soldierName, int destinationTeamId, int destinationSquadId, bool forceKilled)
        {
            PlayerProfile player = getPlayerProfile(soldierName);
            if (player == null)
                return;

            player.state = PlayerState.dead;

        }



        public void OnGlobalChat(string strSpeaker, string strMessage)
        {
            if (isInGameCommand(strMessage))
                inGameCommand(strSpeaker, strMessage);

            PlayerProfile player = getPlayerProfile(strSpeaker);
            if (player != null)
                player.updateLastChat();
        }


        public void OnTeamChat(string strSpeaker, string strMessage, int iTeamID)
        {
            if (isInGameCommand(strMessage))
                inGameCommand(strSpeaker, strMessage);

            PlayerProfile player = getPlayerProfile(strSpeaker);
            if (player != null)
                player.updateLastChat();
        }


        public void OnSquadChat(string strSpeaker, string strMessage, int iTeamID, int iSquadID)
        {
            if (isInGameCommand(strMessage))
                inGameCommand(strSpeaker, strMessage);

            PlayerProfile player = getPlayerProfile(strSpeaker);
            if (player != null)
                player.updateLastChat();
        }


        private bool isInGameCommand(string str)
        {
            if (Regex.Match(str, @"^\s*[@/!]").Success)
                return true;

            return false;
        }

        public void OnLevelStarted()
        {
            DebugWrite("Level starting!", 3);
            level_started = true;
        }


        public void OnPunkbusterplayerStatsCmd(CPunkbusterInfo cpbiPlayer)
        {
        }


        public void processNewPlayer(CPunkbusterInfo cpbiPlayer)
        {
            if (this.players.ContainsKey(cpbiPlayer.SoldierName))
                this.players[cpbiPlayer.SoldierName].pbinfo = cpbiPlayer;
            else
            {
                lock (mutex)
                {

                    // add new player to the queue, and wake the stats fetching loop
                    if (!(new_player_queue.ContainsKey(cpbiPlayer.SoldierName) ||
                          players.ContainsKey(cpbiPlayer.SoldierName) ||
                          new_players_batch.ContainsKey(cpbiPlayer.SoldierName)))
                    {
                        ConsoleWrite("Queueing ^b" + cpbiPlayer.SoldierName + "^n for stats fetching");
                        new_player_queue.Add(cpbiPlayer.SoldierName, cpbiPlayer);
                        wake_handle.Set();
                    }

                }
            }
        }


        Dictionary<String, PlayerProfile> new_players_batch = new Dictionary<string, PlayerProfile>();

        Object mutex = new Object();
        EventWaitHandle scratch_handle = new EventWaitHandle(false, EventResetMode.ManualReset);
        public void stats_fetching_loop()
        {
            ConsoleWrite("Starting stats fetching thread");
            getPlayerList(); 
            while (true)
            {
                if (new_player_queue.Count == 0)
                {
                    // if there are no more players, put yourself to sleep
                    ConsoleWrite("No new players, stats fetching thread going to sleep");
                    wake_handle.Reset();
                    wake_handle.WaitOne();
                    ConsoleWrite("Stats fetching thread is now awake!");
                }


                InsaneBalancer plugin = this;

                while (new_player_queue.Count > 0)
                {
                    if (!plugin_enabled)
                        break;

                    List<String> keys = new List<string>(new_player_queue.Keys);

                    String name = keys[keys.Count - 1];

                    CPunkbusterInfo info = null;
                    new_player_queue.TryGetValue(name, out info);

                    if (info == null)
                        continue;

                    // make sure I am the only one modifying these dictionarie at this time
                    lock (mutex)
                    {
                        if (new_player_queue.ContainsKey(name))
                            new_player_queue.Remove(name);

                        if (!new_players_batch.ContainsKey(name))
                            new_players_batch.Add(name, null);
                    }

                    String msg = new_player_queue.Count + " more player" + ((new_player_queue.Count > 1) ? "s" : "") + " in queue";
                    if (new_player_queue.Count == 0)
                        msg = "no more players in queue";

                    plugin.ConsoleWrite("Getting battlelog stats for ^b" + name + "^n, " + msg);
                    if (new_players_batch.ContainsKey(info.SoldierName))
                        new_players_batch[name] = new PlayerProfile(plugin, info);
                }

                // abort the thread if the plugin was disabled
                if (!plugin_enabled)
                {
                    plugin.ConsoleWrite("detected that plugin was disabled, aborting stats fetching thread");
                    lock (mutex)
                    {
                        new_player_queue.Clear();
                        new_players_batch.Clear();
                        scratch_list.Clear();
                    }
                    return;
                }

                ConsoleWrite("Done fetching stats, " + new_players_batch.Count + " player" + ((new_players_batch.Count > 1) ? "s" : "") + " in new batch, waiting for players list now");
                scratch_handle.Reset();
                getPlayerList();
                scratch_handle.WaitOne();
                scratch_handle.Reset();
                lock (mutex)
                {
                    // remove the nulls, and the ones that left
                    List<String> players_to_remove = new List<string>();
                    foreach (KeyValuePair<String, PlayerProfile> pair in new_players_batch)
                        if (pair.Value == null || !scratch_list.Contains(pair.Key))
                            if (!players_to_remove.Contains(pair.Key))
                            {
                                DebugWrite("Looks like ^b" + pair.Key + "^n left, removing him from new batch", 3);
                                players_to_remove.Add(pair.Key);
                            }
                        

                    // now remove them
                    foreach (String pname in players_to_remove)
                        if (new_players_batch.ContainsKey(pname))
                            new_players_batch.Remove(pname);

                    if (new_players_batch.Count > 0)
                        ConsoleWrite("Queue exhausted, will insert now a batch of " + new_players_batch.Count + " player" + ((new_players_batch.Count>1) ? "s" : ""));
                    foreach (KeyValuePair<String, PlayerProfile> pair in new_players_batch)
                        if (pair.Value != null && scratch_list.Contains(pair.Key))
                            plugin.players.Add(pair.Key, pair.Value);

                    new_players_batch.Clear();
                }

            }

        }

        Object info_mutex = new Object();
        public void OnServerInfo(CServerInfo csiServerInfo)
        {
            lock (info_mutex)
            {
                this.serverInfo = csiServerInfo;
            }
        }



        public void OnPlayerTeamChange(string soldierName, int teamId, int squadId)
        {
            PlayerProfile player = getPlayerProfile(soldierName);
            if (player == null)
                return;

            player.state = PlayerState.dead;
            player.setTeamId(teamId);
            player.setSquadId(squadId);

        }


        public void OnPlayerSquadChange(string strSoldierName, int iTeamID, int iSquadID)
        {
            PlayerProfile player = getPlayerProfile(strSoldierName);
            if (player == null)
                return;

            player.setSquadId(iSquadID);
            player.setTeamId(iTeamID);
        }

        public void OnplayersStatsCmd(List<CPlayerInfo> lstPlayers, CPlayerSubset cpsSubset)
        {

            if (cpsSubset.Subset == CPlayerSubset.PlayerSubsetType.All)
                foreach (CPlayerInfo cpiPlayer in lstPlayers)
                    if (this.players.ContainsKey(cpiPlayer.SoldierName))
                        this.players[cpiPlayer.SoldierName].updateInfo(cpiPlayer);

            /* fail safe to get the maximum number of players in server */
            if (lstPlayers.Count > max_player_count)
                max_player_count = lstPlayers.Count;
        }


        public int getPerMapInterval()
        {

            String key = getPerMapKey();

            if (key.Length > 0)
                return getIntegerVarValue(key);

            return 0;
        }

        public String getPerMapKey()
        {
            string mode = serverInfo.GameMode.ToLower().Trim();
            string map = serverInfo.Map.ToLower().Trim();

            if (maps.ContainsKey(map) && modes.ContainsKey(mode))
                return modes[mode] + "_" + maps[map];

            return "";
        }

        public bool checkRoundBalance()
        {
            int round_interval = this.getIntegerVarValue("round_interval");
            int round_total = serverInfo.TotalRounds;
            int round_current = serverInfo.CurrentRound + 1;
            string map = serverInfo.Map.ToLower();

            int per_map_interval = getPerMapInterval();

            /* if user set a value per-map, use that instead */
            if (per_map_interval > 0)
            {
                ConsoleWrite("Using round_interval value of " + per_map_interval + " for map " + getPerMapKey());
                round_interval = per_map_interval;
            }

            if (round_interval > round_total)
            {
                ConsoleWrite("^1^bWARNING^0^n: ^bround_interval(" + round_interval + ")^n is greater than total ^brounds(" + round_total + ")^n for ^bmap(" + map + ")^n^0");
                ConsoleWrite("setting ^bround_interval^n to ^b" + round_total + "^n internally for ^bmap(" + map + ")^n^0");
                round_interval = round_total;
            }


            ConsoleWrite("End of round detected");
            ConsoleWrite("Current round is ^b" + round_current + "^n/^b" + round_total + "^n,");
            ConsoleWrite("Round balance interval is ^b" + round_interval + "^n^0");

            if (!getBooleanVarValue("balance_round"))
                return false;

            if (round_current % round_interval == 0)
                return true;

            return false;
        }


        public static string TN(int teamNo)
        {

            switch (teamNo)
            {
                case 0:
                    return "Neutral";
                case 1:
                    return "US";
                case 2:
                    return "RU";
                default:
                    return "Unknown";
            }
        }


        public static string SQN(int squadNo)
        {

            switch (squadNo)
            {
                case 0:
                    return "Neutral";
                case 1:
                    return "Alpha";
                case 2:
                    return "Bravo";
                case 3:
                    return "Charlie";
                case 4:
                    return "Delta";
                case 5:
                    return "Echo";
                case 6:
                    return "Foxtrot";
                case 7:
                    return "Golf";
                case 8:
                    return "Hotel";
                case 9:
                    return "India";
                case 10:
                    return "Juliet";
                case 11:
                    return "Kilo";
                case 12:
                    return "Lima";
                case 13:
                    return "Mike";
                case 14:
                    return "November";
                case 15:
                    return "Oscar";
                case 16:
                    return "Papa";
                case 17:
                    return "X-Alpha";
                case 18:
                    return "X-Bravo";
                case 19:
                    return "X-Charlie";
                case 20:
                    return "X-Delta";
                case 21:
                    return "X-Echo";
                case 22:
                    return "X-Foxtrot";
                case 23:
                    return "X-Golf";
                case 24:
                    return "X-Hotel";
                case 25:
                    return "X-India";
                case 26:
                    return "X-Juliet";
                case 27:
                    return "X-Kilo";
                case 28:
                    return "X-Lima";
                case 29:
                    return "X-Mike";
                case 30:
                    return "X-November";
                case 31:
                    return "X-Oscar";
                case 32:
                    return "X-Papa";

                default:
                    if (squadNo > 16 && squadNo <= 32)
                        return "S-" + squadNo;
                    else
                        return "Unknown";
            }
        }

        public double getRoundMinutes()
        {
            return utc.Subtract(startRoundTime).TotalMinutes;
        }


        public void delayedRoundBalance()
        {

            bool original_state = getBooleanVarValue("balance_live");

            if (original_state)
            {
                ConsoleWrite("Temporarily disabling live balancer, for round-over");
                setBooleanVarValue("balance_live", false);
            }

            try
            {
                wait_state = true;
                Thread.Sleep(getIntegerVarValue("round_wait_time") * 1000);
                wait_state = false;

                ConsoleWrite("round-over, ^b" + getIntegerVarValue("round_wait_time") + "^n seconds wait time expired");
                if (round_balancer)
                    balanceRound(win_teamId);
                restartWaitState(utc);
                resetPlayerStats();
                round_balancer = false;

            }
            catch (Exception e)
            {
                dump_exception(e);
            }


            if (original_state)
            {
                ConsoleWrite("Re-enabling live balancing");
                setBooleanVarValue("balance_live", true);
            }
        }

        public void OnRoundOver(int iWinningTeamID)
        {
            round_balancer = checkRoundBalance();
            win_teamId = iWinningTeamID;
            lose_teamId = getOpposingTeamId(win_teamId);
            level_started = true;

            if (!getBooleanVarValue("balance_round"))
            {
                ConsoleWrite("round-over, but ^bbalance_round^n is not enabled");
                round_balancer = false;
                return;
            }


            ConsoleWrite("round-over, waiting for ^b" + getIntegerVarValue("round_wait_time") + "^n seconds");
            Thread sleeper = new Thread(new ThreadStart(delayedRoundBalance));
            sleeper.Start();

        }

        public override void OnPlayerSpawned(string soldierName, Inventory spawnedInventory)
        {
            PlayerProfile player = getPlayerProfile(soldierName);
            if (player != null)
            {
                player.updateLastSpawn();
                player.state = PlayerState.alive;
            }

        }


        private void resetPlayerStats()
        {

            List<PlayerProfile> players_list = getPlayersProfile("");
            foreach (PlayerProfile player in players_list)
            {
                player.resetStats();
            }

            /* reset the fail-safe counter */
            max_player_count = 0;

        }



        private void SendPlayerMessage(string soldierName, string message)
        {
            if (getBooleanVarValue("quiet_mode") && !isAdmin(soldierName))
                return;

            if (soldierName == null)
                return;

            /* Temporarily disable player messages until DICE 
             * enables individual player messages
             */

            //ExecCommand("admin.say", message, "player", soldierName);
        }


        private void SendGlobalMessage(string message)
        {
            if (getBooleanVarValue("quiet_mode"))
                SendConsoleMessage(message);
            else
                ExecCommand("admin.say", message, "all");

        }

        private void SendConsoleMessage(string name, string msg)
        {
            List<string> admin_list = getAdminList();

            ConsoleWrite(msg);
            msg = Regex.Replace(msg, @"\^[0-9a-zA-Z]", "");

            if (name != null)
                SendPlayerMessage(name, msg);


        }

        private void SendConsoleMessage(string msg)
        {
            List<string> admin_list = getAdminList();
            ConsoleWrite(msg);

            msg = Regex.Replace(msg, @"\^[0-9a-zA-Z]", "");



            foreach (string name in admin_list)
            {
                PlayerProfile pp = this.getPlayerProfile(name);
                if (pp != null)
                {
                    SendPlayerMessage(pp.name, msg);
                }
            }

        }


        private void KickPlayerWithMessage(PlayerProfile player, string message)
        {
            if (player == null)
                return;

            player.state = PlayerState.kicked;
            this.ExecuteCommand("procon.protected.send", "admin.kickPlayer", player.name, message);
            if (players.ContainsKey(player.name))
                players.Remove(player.name);
        }

        private void inGameCommand(string cmd)
        {
            inGameCommand(getAdminList()[0], cmd);
        }

        private void inGameCommand(string sender, string cmd)
        {

            try
            {

                //Player commands
                Match adminMovePlayerMatch = Regex.Match(cmd, @"\s*[!@/]\s*move\s+([^ ]+)", RegexOptions.IgnoreCase);
                Match movePlayerMatch = Regex.Match(cmd, @"\s*[!@/]\s*move", RegexOptions.IgnoreCase);



                Match showPlayerRoundStatsMatch = Regex.Match(cmd, @"\s*[!@/]\s*show\s+round\s+stats\s+([^ ]+)", RegexOptions.IgnoreCase);
                Match showRoundStatsMatch = Regex.Match(cmd, @"\s*[!@/]\s*show\s+round\s+stats", RegexOptions.IgnoreCase);

                Match showPlayerOnlineStatsMatch = Regex.Match(cmd, @"\s*[!@/]\s*show\s+online\s+stats\s+([^ ]+)", RegexOptions.IgnoreCase);
                Match showOnlineStatsMatch = Regex.Match(cmd, @"\s*[!@/]\s*show\s+online\s+stats", RegexOptions.IgnoreCase);

                Match showIdlePlayersMatch = Regex.Match(cmd, @"\s*[!@/]\s*show\s+idle", RegexOptions.IgnoreCase);
                Match wlistInfoPlayerMatch = Regex.Match(cmd, @"\s*[!@/]\s*wlist_info\s+([^ ]+)", RegexOptions.IgnoreCase);

                Match stopBalancerMatch = Regex.Match(cmd, @"\s*[!@/]\s*stop\s+check", RegexOptions.IgnoreCase);
                Match startBalancerMatch = Regex.Match(cmd, @"\s*[!@/]\s*start\s+check", RegexOptions.IgnoreCase);
                Match balanceLiveMatch = Regex.Match(cmd, @"\s*[!@/]\s*balance\s+live", RegexOptions.IgnoreCase);
                Match balanceRoundMatch = Regex.Match(cmd, @"\s*[!@/]\s*balance\s+round", RegexOptions.IgnoreCase);


                //Setting/Getting variables
                Match setVarValueMatch = Regex.Match(cmd, @"\s*[!@/]\s*set\s+([^ ]+)\s+(.+)", RegexOptions.IgnoreCase);
                Match setVarValueEqMatch = Regex.Match(cmd, @"\s*[!@/]\s*set\s+([^ ]+)\s*=\s*(.+)", RegexOptions.IgnoreCase);
                Match setVarValueToMatch = Regex.Match(cmd, @"\s*[!@/]\s*set\s+([^ ]+)\s+to\s+(.+)", RegexOptions.IgnoreCase);
                Match setVarTrueMatch = Regex.Match(cmd, @"\s*[!@/]\s*set\s+([^ ]+)", RegexOptions.IgnoreCase);
                Match getVarValueMatch = Regex.Match(cmd, @"\s*[!@/]\s*get\s+([^ ]+)", RegexOptions.IgnoreCase);
                Match enableMatch = Regex.Match(cmd, @"\s*[!@/]\s*enable\s+(.+)", RegexOptions.IgnoreCase);
                Match disableMatch = Regex.Match(cmd, @"\s*[!@/]\s*disable\s+(.+)", RegexOptions.IgnoreCase);

                //ConsoleWrite("Command run " + cmd + ", Matched: " + enableMatch.Success);
                //Information
                Match pluginSettingsMatch = Regex.Match(cmd, @"\s*[!@/]\s*settings", RegexOptions.IgnoreCase);


                bool senderIsAdmin = isAdmin(sender);

                DateTime now = utc;
                if (showIdlePlayersMatch.Success && senderIsAdmin)
                    showIdlePlayers(sender);
                if (wlistInfoPlayerMatch.Success && senderIsAdmin)
                    wlistInfoPlayer(sender, wlistInfoPlayerMatch.Groups[1].Value);
                else if (startBalancerMatch.Success && senderIsAdmin)
                    startBalancerCmd(sender, now);
                else if (stopBalancerMatch.Success && senderIsAdmin)
                    stopBalancerCmd(sender, now);
                else if (showPlayerRoundStatsMatch.Success && senderIsAdmin)
                    showPlayerRoundStatsCmd(sender, showPlayerRoundStatsMatch.Groups[1].Value);
                else if (showRoundStatsMatch.Success && senderIsAdmin)
                    showPlayerRoundStatsCmd(sender, null);

                else if (showPlayerOnlineStatsMatch.Success && senderIsAdmin)
                    showPlayerOnlineStatsCmd(sender, showPlayerOnlineStatsMatch.Groups[1].Value);
                else if (showOnlineStatsMatch.Success && senderIsAdmin)
                    showPlayerOnlineStatsCmd(sender, null);

                else if (balanceLiveMatch.Success && senderIsAdmin)
                    balanceLiveCmd(sender, now);
                else if (balanceRoundMatch.Success && senderIsAdmin)
                    balanceRoundCmd(sender, now);
                else if (adminMovePlayerMatch.Success && senderIsAdmin)
                    movePlayerCmd(sender, adminMovePlayerMatch.Groups[1].Value);
                else if (movePlayerMatch.Success)
                    movePlayerCmd(sender);
                else if (setVarValueEqMatch.Success && senderIsAdmin)
                    setVariableCmd(sender, setVarValueEqMatch.Groups[1].Value, setVarValueEqMatch.Groups[2].Value);
                else if (setVarValueToMatch.Success && senderIsAdmin)
                    setVariableCmd(sender, setVarValueToMatch.Groups[1].Value, setVarValueToMatch.Groups[2].Value);
                else if (setVarValueMatch.Success && senderIsAdmin)
                    setVariableCmd(sender, setVarValueMatch.Groups[1].Value, setVarValueMatch.Groups[2].Value);
                else if (setVarTrueMatch.Success && senderIsAdmin)
                    setVariableCmd(sender, setVarTrueMatch.Groups[1].Value, "1");
                else if (getVarValueMatch.Success && senderIsAdmin)
                    getVariableCmd(sender, getVarValueMatch.Groups[1].Value);
                else if (enableMatch.Success && senderIsAdmin)
                    enableVarGroupCmd(sender, enableMatch.Groups[1].Value);
                else if (disableMatch.Success && senderIsAdmin)
                    disableVarGroupCmd(sender, disableMatch.Groups[1].Value);
                else if (pluginSettingsMatch.Success && senderIsAdmin)
                    pluginSettingsCmd(sender);
            }
            catch (Exception e)
            {
                dump_exception(e);
            }
        }





        private string state2strWHILE(PluginState state)
        {
            if (state.Equals(PluginState.balance))
            {
                return "balancing";
            }
            else if (state.Equals(PluginState.check))
            {
                return "checking";
            }
            else if (state.Equals(PluginState.warn))
            {
                return "warning";
            }
            else if (state.Equals(PluginState.stop))
            {
                return "stopped";
            }
            else if (state.Equals(PluginState.wait))
            {
                return "waiting";
            }

            return "unknown state";
        }



        private void initializeBalancer()
        {
            getServerInfo();
            startStopState(utc);

            // initialize the stats fetching thread
            this.wake_handle = new EventWaitHandle(false, EventResetMode.ManualReset);
            this.stats_fetching_thread = new Thread(new ThreadStart(stats_fetching_loop));
            stats_fetching_thread.Start();

        }


        private void movePlayerCmd(string sender)
        {
            if (teamsUnbalanced())
            {
                SendConsoleMessage(sender, "Teams are un-balanced, cannot move to other team");
                return;
            }

            movePlayerCmd(sender, null);
        }

        private void movePlayerCmd(string sender, string player)
        {
            /* player is moving himself */
            if (player == null)
                player = sender;

            SendConsoleMessage(sender, "moving player " + player);

            PlayerProfile profile = getPlayerProfile(player);

            if (profile == null)
            {
                SendConsoleMessage(sender, "cannot find profile for " + player);
                return;
            }

            if (profile.getTeamId() == 0)
            {
                SendConsoleMessage(sender, "cannot move " + player + " from neutral team");
                return;
            }

            int opposite = (profile.getTeamId() == 1) ? 2 : 1;
            movePlayer(profile, opposite, 0, false, true);
        }


        private void startWaitSate(DateTime now)
        {
            startWaitSate(null, now);
        }



        private void startWarnState(DateTime now)
        {
            startWarnState(null, now);
        }

        private void startWarnState(string sender, DateTime now)
        {
            if (!isPluginChecking())
            {
                SendConsoleMessage(sender, "plugin is in " + pluginState.ToString() + " state");
                return;
            }

            pluginState = PluginState.warn;
            setStartTime(pluginState, now.AddSeconds(1));

            DebugWrite("^b" + pluginState + "^n state started " + getStartTime(pluginState).ToString(), 1);
        }

        private void startWaitSate(string sender, DateTime now)
        {

            if (!isPluginStopped())
            {
                SendConsoleMessage(sender, "cannot start while balancer is in " + pluginState.ToString() + " state");
                return;
            }

            if (sender != null)
                setPluginVarValue("auto_start", "true");


            pluginState = PluginState.wait;
            setStartTime(pluginState, now.AddSeconds(1));


            SendConsoleMessage(sender, "^b" + pluginState + "^n state started " + getStartTime(pluginState).ToString());
        }

        private void startStopState(DateTime now)
        {
            startStopState(null, now);
        }

        private void startStopState(string sender, DateTime now)
        {
            virtual_mode = false;
            round_balancer = false;
            level_started = false;
            check_state_phase = 0;

            if (sender != null)
                setPluginVarValue("auto_start", "false");

            pluginState = PluginState.stop;
            setStartTime(pluginState, now.AddSeconds(1));

            SendConsoleMessage(sender, "^b" + pluginState + "^n state started " + getStartTime(pluginState).ToString());
        }


        public void balanceLiveCmd(string sender, DateTime now)
        {

            balanceLive(now);
            restartWaitState(now);
        }

        public void balanceRoundCmd(string sender, DateTime now)
        {
            try
            {
                balanceRound(1);
                restartWaitState(utc);
                resetPlayerStats();
            }
            catch (Exception e)
            {
                dump_exception(e);
            }

        }

        private bool isPlayerIdle(PlayerProfile player)
        {
            int last_kill_time = getIntegerVarValue("last_kill_time");
            int last_death_time = getIntegerVarValue("last_death_time");
            int last_chat_time = getIntegerVarValue("last_chat_time");
            int last_spawn_time = getIntegerVarValue("last_spawn_time");
            int last_score_time = getIntegerVarValue("last_score_time");


            if (player.getLastKill() > last_kill_time &&
                player.getLastDeath() > last_death_time &&
                player.getLastChat() > last_chat_time &&
                player.getLastSpawn() > last_spawn_time &&
                player.getLastScore() > last_score_time)
                return true;

            return false;

        }

        public void showIdlePlayers(string sender)
        {
            List<PlayerProfile> players_list = getPlayersProfile("");

            List<PlayerProfile> list = new List<PlayerProfile>();

            foreach (PlayerProfile player in players_list)
                if (isPlayerIdle(player))
                    list.Add(player);


            SendConsoleMessage(sender, " == " + list.Count + " idle players (watching for last " + Math.Round(getRoundMinutes(), 2) + " minutes)  ==");
            foreach (PlayerProfile player in list)
                SendConsoleMessage(sender, player + ": " + player.getIdleStatistics());
        }


        public void wlistInfoPlayer(string sender, string pname)
        {

            PlayerProfile player = getPlayerProfile(pname);
            if (player == null)
            {
                SendConsoleMessage(sender, "^1^bWARNING^n^0: could not find ^b" + pname + "^n in game");
                return;
            }

            SendConsoleMessage(sender, " == White List Info for " + pname + "  ==");

            List<String> list_names = new List<string>();
            list_names.Add("player_move_wlist");
            list_names.Add("clan_move_wlist");
            list_names.Add("player_safe_wlist");
            list_names.Add("clan_move_wlist");
            list_names.Add("player_safe_wlist");
            list_names.Add("clan_safe_wlist");

            foreach (String list in list_names)
            {
                Boolean inlist = isPlayerInWhiteList(player, list);
                String inlist_str = (inlist) ? "^b" + inlist.ToString() + "^n" : inlist.ToString();
                SendConsoleMessage(sender, list + " = " + inlist_str);
            }

        }


        public void showPlayerRoundStatsCmd(string sender, string player_name)
        {
            List<PlayerProfile> players_list;
            if (player_name == null)
                players_list = getPlayersProfile("");
            else
                players_list = getPlayersProfile(player_name);


            if (players_list.Count == 0)
                return;

            int i = 1;
            SendConsoleMessage(sender, " == Round Statistics ( " + Math.Round(getRoundMinutes(), 2) + " minutes) ==");
            foreach (PlayerProfile player in players_list)
            {
                SendConsoleMessage(sender, i + ". " + player + ": " + player.getRoundStatistics());
                i++;
            }
        }

        public void showPlayerOnlineStatsCmd(string sender, string player_name)
        {
            List<PlayerProfile> players_list;
            if (player_name == null)
                players_list = getPlayersProfile("");
            else
                players_list = getPlayersProfile(player_name);


            if (players_list.Count == 0)
                return;

            int i = 1;

            SendConsoleMessage(sender, " == Online Statistics ==");
            foreach (PlayerProfile player in players_list)
            {
                SendConsoleMessage(sender, i + ". " + player + ": " + player.getOnlineStatistics());
                i++;
            }
        }

        private void startBalancerCmd(string sender, DateTime now)
        {
            setBooleanVarValue("balance_live", true);
            startWaitSate(now);
        }

        private void stopBalancerCmd(string sender, DateTime now)
        {
            setBooleanVarValue("balance_live", false);
            startStopState(now);
        }

        private void restartWaitState(DateTime now)
        {
            pluginState = PluginState.wait;
            setStartTime(pluginState, now.AddSeconds(1));
            DebugWrite("^b" + pluginState.ToString() + "^n state re-started " + getStartTime(pluginState).ToString() + "^0", 1);
        }



        private void genericSayAnnounce(List<string> messages)
        {
            if (messages.Count == 0)
                return;

            int remain_time = getRemainingTime(utc, pluginState);

            for (int i = 0; i < messages.Count; i++)
            {
                string msg = messages[i];
                msg = msg.Replace("%time%", remain_time.ToString());
                SendGlobalMessage(msg);
            }

        }



        private void warnAnnounce(int display_time)
        {
            if (!isPluginWarning())
                return;

            DebugWrite("sending ^b" + pluginState.ToString() + "^n announcement", 1);

            List<string> msg = new List<string>();
            msg.Add("Teams are unbalanced");
            msg.Add("Autobalancer starts in %time% secs");

            if (getBooleanVarValue("warn_say"))
                genericSayAnnounce(msg);
        }


        private void warnCountdown()
        {
            if (!isPluginWarning())
                return;

            DebugWrite("sending ^b" + pluginState.ToString() + "^n countdown", 1);

            int remain_time = getRemainingTime(utc, PluginState.warn);
            string msg = "Autobalancer starts in " + remain_time.ToString() + "!";

            if (getBooleanVarValue("warn_say"))
                SendGlobalMessage(msg);
        }

        private void enableVarGroupCmd(string sender, string group)
        {
            if (group.CompareTo("plugin") == 0)
            {
                ConsoleWrite("Disabling plugin");
                this.ExecuteCommand("procon.plugin.enable", "InsaneBalancer", "false");
            }
            enablePluginVarGroup(sender, group);
        }

        private void disableVarGroupCmd(string sender, string group)
        {
            if (group.CompareTo("plugin") == 0)
            {
                ConsoleWrite("Enabling plugin");
                this.ExecuteCommand("procon.plugin.enable", "InsaneBalancer", "true");
            }

            disablePluginVarGroup(sender, group);
        }

        private bool setPluginVarGroup(string sender, string group, string val)
        {
            String msg = "";
            if (group == null)
            {
                msg = "no variables to enable";
                ConsoleWrite(msg);
                SendConsoleMessage(sender, msg);
                return false;
            }


            group = group.Replace(";", ",");
            List<string> vars = new List<string>(Regex.Split(group, @"\s*,\s*", RegexOptions.IgnoreCase));
            foreach (string var in vars)
            {
                if (setPluginVarValue(sender, var, val))
                {
                    msg = var + " set to \"" + val + "\"";
                    SendConsoleMessage(sender, msg);
                }

            }
            return true;
        }

        private bool enablePluginVarGroup(string sender, string group)
        {
            //search for all variables matching
            List<string> vars = getVariableNames(group);
            String msg = "";
            if (vars.Count == 0)
            {
                msg = "no variables match \"" + group + "\"";
                //ConsoleWrite(msg);
                SendConsoleMessage(sender, msg);
                return false;
            }

            return setPluginVarGroup(sender, String.Join(",", vars.ToArray()), "true");
        }

        private List<string> getVariableNames(string group)
        {
            List<string> names = new List<string>();
            List<string> list = new List<string>(Regex.Split(group, @"\s*,\s*"));
            List<string> vars = getPluginVars();
            foreach (string search in list)
            {
                foreach (string var in vars)
                {
                    if (var.Contains(search))
                        if (!names.Contains(var))
                            names.Add(var);
                }
            }

            return names;
        }

        private bool disablePluginVarGroup(string sender, string group)
        {
            //search for all variables matching
            List<string> vars = getVariableNames(group);

            if (vars.Count == 0)
            {
                SendConsoleMessage(sender, "no variables match \"" + group + "\"");
                return false;
            }
            return setPluginVarGroup(sender, String.Join(",", vars.ToArray()), "false");
        }

        private void getVariableCmd(string sender, string var)
        {
            string val = getPluginVarValue(sender, var);

            if (var.Equals("pass"))
                val = Regex.Replace(val, @".", "*");

            String msg = var + " = " + val;

            //ConsoleWrite(msg);
            SendConsoleMessage(sender, msg);
        }




        private void setVariableCmd(string sender, string var, string val)
        {

            if (setPluginVarValue(sender, var, val))
            {
                SendConsoleMessage(sender, var + " set to \"" + val + "\"");
            }
        }

        private void pluginSettingsCmd(string sender)
        {
            SendConsoleMessage(sender, " == Insane Balancer Settings == ");
            foreach (string var in getPluginVars())
            {
                SendConsoleMessage(sender, var + " = " + getPluginVarValue(sender, var));
            }
        }


        public bool stringValidator(string var, string value)
        {
            if (var.CompareTo("round_sort") == 0)
            {
                if (!strAssertSort(value))
                    return false;
            }
            if (var.CompareTo("live_sort") == 0)
            {
                if (!strAssertSort(value))
                    return false;
            }
            return true;
        }

        public bool commandValidator(string var, string value)
        {

            try
            {
                inGameCommand(value);
            }
            catch (Exception e)
            {
                dump_exception(e);
            }
            return false;
        }


        public List<String> getAllowedSorts()
        {
            List<string> sort_methods = new List<string>();


            sort_methods.Add("kdr_asc_round");
            sort_methods.Add("kdr_desc_round");
            sort_methods.Add("spm_asc_round");
            sort_methods.Add("spm_desc_round");
            sort_methods.Add("kpm_asc_round");
            sort_methods.Add("kpm_desc_round");
            sort_methods.Add("score_asc_round");
            sort_methods.Add("score_desc_round");
            sort_methods.Add("time_asc_round");
            sort_methods.Add("time_desc_round");


            sort_methods.Add("kdr_asc_online");
            sort_methods.Add("kdr_desc_online");
            sort_methods.Add("kpm_asc_online");
            sort_methods.Add("kpm_desc_online");
            sort_methods.Add("spm_asc_online");
            sort_methods.Add("spm_desc_online");
            sort_methods.Add("kills_asc_online");
            sort_methods.Add("kills_desc_online");
            sort_methods.Add("deaths_asc_online");
            sort_methods.Add("deaths_desc_online");
            sort_methods.Add("skill_asc_online");
            sort_methods.Add("skill_desc_online");
            sort_methods.Add("quits_asc_online");
            sort_methods.Add("quits_desc_online");
            sort_methods.Add("accuracy_asc_online");
            sort_methods.Add("accuracy_desc_online");
            sort_methods.Add("score_asc_online");
            sort_methods.Add("score_desc_online");
            sort_methods.Add("rank_asc_online");
            sort_methods.Add("rank_desc_online");

            sort_methods.Add("random_value");

            return sort_methods;
        }


        public bool strAssertSort(string value)
        {
            if (value == null)
                return false;


            List<String> sort_methods = getAllowedSorts();

            if (!sort_methods.Contains(value))
            {
                SendConsoleMessage("^1^bERROR^0^n: ^b" + value + "^n is not a valid sort method ^0");
                SendConsoleMessage("valid sort methods are: ^b" + String.Join("^0,^b ", sort_methods.ToArray()) + "^0");
                return false;
            }
            return true;
        }

        public bool booleanValidator(string var, bool value)
        {
            return true;
        }


        bool boolAssertNE(string var, bool value, string cmp)
        {
            bool cmp_value = getBooleanVarValue(cmp);

            if (!(value != cmp_value))
            {
                ConsoleWrite("^1^bERROR^0^n:  cannot set ^b" + var + "^n to ^b" + value.ToString() + "^n while ^b" + cmp + "^n is set to ^b" + cmp_value.ToString() + "^n^0");
                return false;
            }
            return true;
        }

        public bool integerValidator(string var, int value)
        {

            if (var.CompareTo("warn_msg_interval_time") == 0)
            {
                if (!intAssertGTE(var, value, 0) ||
                    !intAssertLTE(var, value, "warn_msg_total_time"))
                    return false;
            }

            if (var.CompareTo("warn_msg_countdown_time") == 0)
            {
                if (!intAssertGTE(var, value, 0) ||
                    !intAssertLTE(var, value, "warn_msg_total_time"))
                    return false;
            }

            if (var.CompareTo("warn_msg_total_time") == 0)
            {
                if (!intAssertGTE(var, value, 0) ||
                    !intAssertGTE(var, value, "warn_msg_interval_time") ||
                    !intAssertGTE(var, value, "warn_msg_countdown_time"))
                    return false;
            }

            if (var.CompareTo("warn_msg_display_time") == 0)
            {
                if (!intAssertGTE(var, value, 0) ||
                    !intAssertLTE(var, value, "warn_msg_total_time"))
                    return false;
            }

            if (var.CompareTo("balance_threshold") == 0 ||
                var.CompareTo("wait_death_count") == 0)
            {
                if (!intAssertGT(var, value, 0))
                    return false;
            }

            if (var.CompareTo("round_interval") == 0)
            {

                if (!intAssertGT(var, value, 0))
                    return false;

                if (serverInfo == null)
                    return true;

                if (value > serverInfo.TotalRounds)
                {
                    SendConsoleMessage("^1^bERROR^0^n: ^b" + var + "(" + value + ")^n must be less than or equal than the total number of ^brounds(" + serverInfo.TotalRounds + ")^n per ^bmap(" + serverInfo.Map.ToLower() + ")^n^0");
                    return false;
                }
            }

            if (var.CompareTo("live_interval_time") == 0)
            {
                if (!intAssertGT(var, value, 0))
                    return false;
            }

            if (var.CompareTo("debug_level") == 0)
            {
                if (!intAssertGTE(var, value, 0))
                    return false;
            }

            return true;
        }


        private bool intAssertLT(string var, int value, int max_value)
        {
            if (!(value < max_value))
            {
                SendConsoleMessage("^1^bERROR^0^n: b" + var + "(" + value + ")^n must be less than  ^b" + max_value + "^n^0");
                return false;
            }

            return true;
        }


        private bool intAssertLTE(string var, int value, int max_value)
        {
            if (!(value <= max_value))
            {
                SendConsoleMessage("^1^bERROR^0^n: ^b" + var + "(" + value + ")^n must be less than or equal to ^b" + max_value + "^n^0");
                return false;
            }

            return true;
        }


        private bool intAssertGT(string var, int value, int min_value)
        {
            if (!(value > min_value))
            {
                SendConsoleMessage("^1^bERROR^0^n: ^b" + var + "(" + value + ")^n must be greater than  ^b" + min_value + "^n^0");
                return false;
            }

            return true;
        }


        private bool intAssertGTE(string var, int value, int min_value)
        {
            if (!(value >= min_value))
            {
                SendConsoleMessage("^1^bERROR^0^n: ^b" + var + "(" + value + ")^n must be greater than or equal to ^b" + min_value + "^n^0");
                return false;
            }

            return true;
        }

        private bool intAssertGTE(string var1, int var1_value, string var2)
        {
            int var2_value = getIntegerVarValue(var2);

            if (!(var1_value >= var2_value))
            {

                SendConsoleMessage("^1^bERROR^0^n: ^b" + var1 + "(" + var1_value + ")^n must be greater than or equal to the value of ^b" + var2 + "(" + var2_value + ")^n");

                return false;
            }

            return true;
        }


        private bool intAssertLTE(string var1, int var1_value, string var2)
        {
            int var2_value = getIntegerVarValue(var2);


            if (!(var1_value <= var2_value))
            {
                SendConsoleMessage("^1^bERROR^0^n: ^b" + var1 + "(" + var1_value + ")^n must be less than or equal to the value of ^b" + var2 + "(" + var2_value + ")^n");
                return false;
            }

            return true;
        }


        private bool setPluginVarValue(string var, string val)
        {
            return setPluginVarValue(null, var, val);
        }

        private bool setPluginVarValue(string sender, string var, string val)
        {
            if (var == null || val == null)
                return false;

            if (!getPluginVars().Contains(var))
            {
                SendConsoleMessage(sender, "Insane Balancer: unknown variable \"" + var + "\"");
                return false;
            }

            /* Parse Boolean Values */
            bool booleanValue = false;
            bool isBooleanValue = true;
            if (Regex.Match(val, @"\s*(1|true|yes)\s*", RegexOptions.IgnoreCase).Success)
                booleanValue = true;
            else if (Regex.Match(val, @"\s*(0|false|no)\s*", RegexOptions.IgnoreCase).Success)
                booleanValue = false;
            else
                isBooleanValue = false;


            /* Parse Integer Values */
            int integerValue = 0;
            //bool isIntegerValue = int.TryParse(val, out integerValue) && integerValue >= 0;
            bool isIntegerValue = int.TryParse(val, out integerValue);

            /* Parse Float Values */
            float floatValue = 0F;
            bool isFloatValue = float.TryParse(val, out floatValue) && floatValue >= 0F;

            /* Parse String List */
            List<string> stringListValue = new List<string>(Regex.Split(val.Replace(";", ",").Replace("|", ","), @"\s*,\s*"));
            bool isStringList = true;

            /* Parse String var */
            string stringValue = val;
            bool isStringValue = (val != null);


            if (isBooleanVar(var))
            {
                if (!isBooleanValue)
                {
                    SendConsoleMessage(sender, "\"" + val + "\" is invalid for " + var);
                    return false;
                }
                setBooleanVarValue(var, booleanValue);
                return true;
            }
            else if (isIntegerVar(var))
            {
                if (!isIntegerValue)
                {
                    SendConsoleMessage(sender, "\"" + val + "\" is invalid for " + var);
                    return false;
                }

                setIntegerVarValue(var, integerValue);
                return true;
            }
            else if (isFloatVar(var))
            {
                if (!isFloatValue)
                {
                    SendConsoleMessage(sender, "\"" + val + "\" is invalid for " + var);
                    return false;
                }

                setFloatVarValue(var, floatValue);
                return true;
            }
            else if (isStringListVar(var))
            {
                if (!isStringList)
                {
                    SendConsoleMessage(sender, "\"" + val + "\"  is invalid for " + var);
                    return false;
                }

                setStringListVarValue(var, stringListValue);
                return true;
            }
            else if (isStringVar(var))
            {
                if (!isStringValue)
                {
                    SendConsoleMessage(sender, "invalid value for " + var);
                    return false;
                }

                setStringVarValue(var, stringValue);
                return true;
            }
            else
            {
                SendConsoleMessage(sender, "Insane Balancer: unknown variable \"" + var + "\"");
                return false;
            }

        }

        private bool isIntegerVar(string var)
        {
            return this.integerVariables.ContainsKey(var);
        }

        private int getIntegerVarValue(string var)
        {
            if (!isIntegerVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return -1;
            }

            return this.integerVariables[var];
        }

        private bool setIntegerVarValue(string var, int val)
        {
            if (!isIntegerVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return false;
            }

            if (hasIntegerValidator(var))
            {
                integerVariableValidator validator = integerVarValidators[var];
                if (validator(var, val) == false)
                    return false;
            }

            this.integerVariables[var] = val;
            return true;
        }

        private bool hasBooleanValidator(string var)
        {
            return booleanVarValidators.ContainsKey(var);
        }

        private bool hasIntegerValidator(string var)
        {
            return integerVarValidators.ContainsKey(var);
        }

        private bool hasStringValidator(string var)
        {
            return stringVarValidators.ContainsKey(var);
        }

        private bool isStringVar(string var)
        {
            return this.stringVariables.ContainsKey(var);
        }


        private string getStringVarValue(string var)
        {
            if (!isStringVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return "";
            }

            return this.stringVariables[var];
        }

        private bool setStringVarValue(string var, string val)
        {
            if (!isStringVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return false;
            }


            if (hasStringValidator(var))
            {
                stringVariableValidator validator = stringVarValidators[var];
                if (validator(var, val) == false)
                    return false;
            }


            string oldval = this.stringVariables[var];
            this.stringVariables[var] = val;

            return true;
        }




        private bool isStringListVar(string var)
        {
            return this.stringListVariables.ContainsKey(var);
        }

        private List<string> getStringListVarValue(string var)
        {
            if (!isStringListVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return new List<string>();
            }

            string[] out_list = Regex.Split(this.stringListVariables[var].Replace(";", ",").Replace("|", ","), @"\s*,\s*");
            return new List<string>(out_list);
        }

        private bool setStringListVarValue(string var, List<string> val)
        {
            if (!isStringListVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return false;
            }

            List<string> cleanList = new List<string>();
            foreach (string item in val)
                if (Regex.Match(item, @"^\s*$").Success)
                    continue;
                else
                    cleanList.Add(item);

            //this.stringListVariables[var] = val;
            this.stringListVariables[var] = String.Join("|", cleanList.ToArray());
            return true;
        }


        private bool isFloatVar(string var)
        {
            return this.floatVariables.ContainsKey(var);
        }

        private float getFloatVarValue(string var)
        {
            if (!isFloatVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return -1F;
            }

            return this.floatVariables[var];
        }

        private bool setFloatVarValue(string var, float val)
        {
            if (!isFloatVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return false;
            }

            this.floatVariables[var] = val;
            return true;
        }


        private bool isBooleanVar(string var)
        {
            return this.booleanVariables.ContainsKey(var);
        }

        private bool getBooleanVarValue(string var)
        {
            if (!isBooleanVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return false;
            }

            return this.booleanVariables[var];
        }

        private bool setBooleanVarValue(string var, bool val)
        {
            if (!isBooleanVar(var))
            {
                SendConsoleMessage("unknown variable \"" + var + "\"");
                return false;
            }

            if (hasBooleanValidator(var))
            {
                booleanVariableValidator validator = booleanVarValidators[var];
                if (validator(var, val) == false)
                    return false;
            }

            this.booleanVariables[var] = val;
            return true;
        }


        private string getPluginVarValue(string var)
        {
            return getPluginVarValue(null, var);
        }

        private string getPluginVarValue(string sender, string var)
        {
            if (!getPluginVars().Contains(var))
            {
                SendConsoleMessage(sender, "Insane Balancer: unknown variable \"" + var + "\"");
                return "";
            }

            if (isBooleanVar(var))
            {
                return getBooleanVarValue(var).ToString();
            }
            else if (isIntegerVar(var))
            {
                return getIntegerVarValue(var).ToString();
            }
            else if (isFloatVar(var))
            {
                return getFloatVarValue(var).ToString();
            }
            else if (isStringListVar(var))
            {
                string lst = list2string(getStringListVarValue(var), "");
                return lst;
            }
            else if (isStringVar(var))
            {
                return getStringVarValue(var);
            }
            else
            {
                SendConsoleMessage(sender, "Insane Balancer: unknown variable \"" + var + "\"");
                return "";
            }
        }

        private List<string> getPluginVars()
        {
            return getPluginVars(false);
        }

        private List<string> getPluginVars(bool hide)
        {
            List<string> vars = new List<string>();


            vars.AddRange(getBooleanPluginVars());
            vars.AddRange(getIntegerPluginVars());
            vars.AddRange(getStringListPluginVars());
            vars.AddRange(getFloatPluginVars());
            vars.AddRange(getStringPluginVars());

            if (hide && !getBooleanVarValue("advanced_mode"))
            {
                foreach (string hidden_var in hiddenVariables)
                    vars.Remove(hidden_var);
            }

            return vars;
        }


        private List<string> getStringPluginVars()
        {
            return new List<string>(this.stringVariables.Keys);
        }


        private List<string> getStringListPluginVars()
        {
            return new List<string>(this.stringListVariables.Keys);
        }


        private List<string> getIntegerPluginVars()
        {
            return new List<string>(this.integerVariables.Keys);
        }

        private List<string> getFloatPluginVars()
        {
            return new List<string>(this.floatVariables.Keys);
        }

        private List<string> getBooleanPluginVars()
        {
            return new List<string>(this.booleanVariables.Keys);
        }

        public string playerstate2stringED(PlayerState state)
        {
            switch (state)
            {
                case PlayerState.alive:
                    return "is alive";
                case PlayerState.dead:
                    return "is dead";
                case PlayerState.kicked:
                    return "was kicked";
                case PlayerState.left:
                    return "left the game";
                case PlayerState.limbo:
                    return "is in limbo";
                default:
                    return "(%player_state%)";
            }

        }


        public string list2string(List<string> list, string glue)
        {

            if (list == null || list.Count == 0)
                return "";
            else if (list.Count == 1)
                return list[0];

            string last = list[list.Count - 1];
            list.RemoveAt(list.Count - 1);

            string str = "";
            foreach (string item in list)
                str += item + ", ";

            return str + glue + last;
        }

        public string list2string(List<string> list)
        {
            return list2string(list, "and ");
        }


        private List<string> getAdminList()
        {
            return getStringListVarValue("admin_list");
        }

        private bool isAdmin(string soldier)
        {
            List<string> admin_list = getAdminList();
            return admin_list.Contains(soldier);
        }


        private PlayerProfile getPlayerProfile(CPlayerInfo info)
        {
            return getPlayerProfile(info.SoldierName);
        }


        private List<PlayerProfile> getPlayersProfile(string name)
        {

            List<PlayerProfile> profiles = new List<PlayerProfile>();
            foreach (KeyValuePair<string, PlayerProfile> pair in this.players)
            {

                if (pair.Value.info == null || pair.Value.info.TeamID < 0 ||
                    pair.Value.wasKicked() || pair.Value.leftGame())
                    continue;

                if (name.Equals(""))
                    profiles.Add(pair.Value);
                else if (pair.Key.ToLower().Contains(name.ToLower()))
                    profiles.Add(pair.Value);
            }


            return profiles;
        }

        private PlayerProfile getPlayerProfile(string name)
        {
            PlayerProfile pp;
            this.players.TryGetValue(name, out pp);
            return pp;
        }


        public override void OnPunkbusterPlayerInfo(CPunkbusterInfo cpbiPlayer)
        {

            if (cpbiPlayer == null)
                return;

            processNewPlayer(cpbiPlayer);
        }

        public void dump_exception(Exception e)
        {
            if (e.GetType().Equals(typeof(ThreadAbortException)))
            {
                Thread.ResetAbort();
                return;
            }
            

            ConsoleWrite("^1^bEXCEPTION^0^n: " + e.GetType() + ": " + e.Message);
            try
            {
                string class_name = "InsaneBalancer";
                // Create a temporary file
                string path = class_name+".dump";


                ConsoleWrite("^1Extra information dumped in file " + path);
                using (FileStream fs = File.Open(path, FileMode.Append))
                {
                    String version = GetPluginVersion();
                    String trace_str = "\n-----------------------------------------------\n";
                    trace_str += "Version: " + class_name +" "+ version + "\n";
                    trace_str += "Date: "+DateTime.Now.ToString()+"\n";
                    trace_str += e.GetType() + ": " + e.Message + "\n\n";
                    trace_str += "Stack Trace: \n"+ e.StackTrace + "\n\n";
                    trace_str += "MSIL Stack Trace:\n";

                    StackTrace trace = new StackTrace(e);
                    StackFrame[] frames = trace.GetFrames();
                    foreach (StackFrame frame in frames)
                        trace_str += "    "+frame.GetMethod() +", IL: " + String.Format("0x{0:X}",frame.GetILOffset())+"\n";
                       
                    
                    Byte[] info = new UTF8Encoding(true).GetBytes(trace_str);
                    fs.Write(info, 0, info.Length);
                }
               

            }
            catch (Exception ex)
            {
                ConsoleWrite("^1^bWARNING^0^n: Unable to dump extra exception information.");
                ConsoleWrite("^1^bEXCEPTION^0^n:  " + ex.TargetSite + ": " + ex.Message);

            }
        }

        public void dump_data(string s)
        {

            try
            {
                // Create a temporary file
                string path = Path.GetRandomFileName() + ".dump";

                ConsoleWrite("^1Dumping information in file " + path);
                using (FileStream fs = File.Open(path, FileMode.OpenOrCreate))
                {
                    Byte[] info = new UTF8Encoding(true).GetBytes(s);
                    fs.Write(info, 0, info.Length);
                }

            }
            catch (Exception ex)
            {
                ConsoleWrite("^1^bWARNING^0^n: Unable to dump extra exception information.");
                ConsoleWrite("^1^bEXCEPTION^0^n:  " + ex.TargetSite + ": " + ex.Message);

            }
        }

        List<String> scratch_list = new List<string>();

        public void updateQueues(List<CPlayerInfo> lstPlayers)
        {
            lock (mutex)
            {
                scratch_handle.Reset();
                // update the scratch list
                scratch_list.Clear();
                foreach (CPlayerInfo info in lstPlayers)
                    if (!scratch_list.Contains(info.SoldierName))
                        scratch_list.Add(info.SoldierName);

                scratch_handle.Set();

                // make a list of players to drop from the stats queue
                List<String> players_to_remove = new List<string>();
                foreach (KeyValuePair<String, CPunkbusterInfo> pair in new_player_queue)
                    if (!scratch_list.Contains(pair.Key) && !players_to_remove.Contains(pair.Key))
                        players_to_remove.Add(pair.Key);

                // now actually drop them from the new players queue
                foreach (String name in players_to_remove)
                    if (new_player_queue.ContainsKey(name))
                    {
                        ConsoleWrite("Looks like ^b" + name + "^n left the server, removing him from stats queue");
                        new_player_queue.Remove(name);
                    }

                // make a list of players to drop from the new players batch
                players_to_remove.Clear();
                foreach (KeyValuePair<String, PlayerProfile> pair in new_players_batch)
                    if (!scratch_list.Contains(pair.Key) && !players_to_remove.Contains(pair.Key))
                        players_to_remove.Add(pair.Key);

                // now actually drop them from the new players batch
                foreach (String name in players_to_remove)
                    if (new_players_batch.ContainsKey(name))
                        new_players_batch.Remove(name);
            }
        }


        public void syncPlayersList(List<CPlayerInfo> lstPlayers)
        {

            lock (mutex)
            {
                // first update the information taht players that still are in list
                foreach (CPlayerInfo cpiPlayer in lstPlayers)
                    if (this.players.ContainsKey(cpiPlayer.SoldierName))
                        this.players[cpiPlayer.SoldierName].updateInfo(cpiPlayer);

                //build a lookup table
                Dictionary<String, bool> player_lookup = new Dictionary<string, bool>();
                foreach (CPlayerInfo pinfo in lstPlayers)
                    if (!player_lookup.ContainsKey(pinfo.SoldierName))
                        player_lookup.Add(pinfo.SoldierName, true);


                List<String> players_to_remove = new List<string>();

                // now make a list of players that will need to be removed
                foreach (KeyValuePair<String, PlayerProfile> pair in players)
                    if (!player_lookup.ContainsKey(pair.Key) && !players_to_remove.Contains(pair.Key))
                        players_to_remove.Add(pair.Key);

                // now actually remove them
                foreach (String pname in players_to_remove)
                    if (players.ContainsKey(pname))
                        players.Remove(pname);
            }
        }


        public override void OnListPlayers(List<CPlayerInfo> lstPlayers, CPlayerSubset cpsSubset)
        {
            if (cpsSubset.Subset != CPlayerSubset.PlayerSubsetType.All)
                return;


            updateQueues(lstPlayers);
            syncPlayersList(lstPlayers);

            /* fail safe to get the maximum number of players in server */
            if (lstPlayers.Count > max_player_count)
                max_player_count = lstPlayers.Count;


            if (check_state_phase == 1)
                startCheckState(utc);
        }

    }
}