Project. Twisted Twitter proxy herd

Background

Wikipedia and its related sites are based on the Wikimedia Architecture, which uses a LAMP platform based on GNU/Linux, Apache, MySQL, and PHP, using multiple, redundant web servers behind a load-balancing virtual router for reliability and performance. For a brief introduction to the Wikimedia Architecture, please see Mark Bergsma, Wikimedia architecture (2007). For a more extensive discussion, please see Domas Mituzas, Wikipedia: Site internals, configuration, code examples and management issues (the workbook), MySQL Users Conference 2007.

While LAMP works fairly well for Wikipedia, let's assume that we are building a new Wikimedia-style service designed for news, where (1) updates to articles will happen far more often, (2) access will be required via various protocols, not just HTTP, and (3) clients will tend to be more mobile. In this new service the application server looks like it will be a bottleneck. From a software point of view our application will turn into too much of a pain to add newer servers (e.g., for access via cell phones, where the cell phones are frequently broadcasting their GPS locations). From a systems point of view the response time looks like it will too slow because the Wikimedia application server is a central bottleneck.

Your team has been asked to look into a different architecture called an "application server herd", where the multiple application servers communicate directly to each other as well as via the core database and caches. The interserver communications are designed for rapidly-evolving data (ranging from small stuff such as GPS-based locations to larger stuff such as ephemeral video data) whereas the database server will still be used for more stable data that is less-often accessed or that requires transactional semantics. For example, you might have three application servers A, B, C such that A talks with B and C but B and C do not talk to each other. However, the idea is that if a user's cell phone posts its GPS location to any one of the application servers then the other servers will learn of the location after one or two interserver transmissions, without having to talk to the database.

You've been delegated to look into the Twisted event-driven networking framework as a candidate for replacing part or all of the Wikimedia platform for your application. Your boss thinks that this might be a good match for the problem, since Twisted's event-driven nature should allow an update to be processed and forwarded rapidly to other servers in the herd. However, he doesn't know how well Twisted will really work in practice. In particular, he wants to know how easy is it to write applications using Twisted, how maintainable and reliable those applications will be, and how well one can glue together new applications to existing ones; he's worried that Python's implementation of type checking, memory management, and multithreading may cause problems for larger applications. He wants you to dig beyond the hype and really understand the pros and cons of using Twisted. He suggests that you write a simple and parallelizable proxy for the Twitter API, as an exercise.

Twitter is a microblogging service that lets users send and receive short messages, called tweets. It is written in a combination of Ruby on Rails and Scala, but interfaces to it are available from many other languages.

You dig around the net and find Twitty Twister, which is a Twisted-based library that lets you access the Twitter API from Twisted applications.

One more thing. Your boss is also thinking of having your team evaluate some other possible frameworks. Next on your list will be Node.js. For now, he wants you to focus on Twisted, but your next project will be evaluate Node.js and he wants you to be thinking about both.

Assignment

Do some research on Twisted as a potential framework for this kind of application. Your research should include an examination of the Twisted source code and documentation, and a small prototype or example code of your own that demonstrates whether Twisted would be an effective way to implement an application server herd. Please base your research on Twisted 12.2.0 (dated 2012-08-31), even if a newer version comes out before the due date; that way we'll all be on the same page. (We suggest using Python 2.7.3, as Twisted 12.2.0 is designed for Python 2.x and it's not likely to work with Python 3.x.)

Your prototype should consist of five servers (with server IDs 'Blake', 'Bryant', 'Gasol', 'Howard', 'Metta') that communicate to each other with the following pattern:

Blake talks with everybody but Gasol and Metta.
Gasol talks with Bryant and with Howard.
Metta talks with Bryant.
Each server should accept TCP connections from clients that emulate mobile devices with IP addresses and DNS names. A client should be able to send its location to the server by sending a message like this:

IAMAT kiwi.cs.ucla.edu +27.5916+086.5640 1353118103.108893381
The first field IAMAT is name of the command where the client tells the server where it is. Its operands are the client ID (in this case, kiwi.cs.ucla.edu), the latitude and longitude in decimal degrees using ISO 6709 notation, and the client's idea of when it sent the message, expressed in POSIX time, which consists of seconds and nanoseconds since 1970-01-01 00:00:00 UTC, ignoring leap seconds; for example, 1353118103.108893381 stands for 2012-11-17 02:08:23.108893381 UTC. A client ID may be any string of non-white-space characters. (A white space character is space, tab, carriage return, newline, formfeed, or vertical tab.) Fields are separated by one or more white space characters and do not contain white space; ignore any leading or trailing white space on the line.

The server should respond to clients with a message like this:

AT Blake +0.563873386 kiwi.cs.ucla.edu +27.5916+086.5640 1353118103.108893381
where AT is the name of the response, Blake is the ID of the server that got the message from the client, +0.563873386 is the difference between the server's idea of when it got the message from the client and the client's time stamp, and the remaining fields are a copy of the IAMAT data. In this example (the normal case), the server's time stamp is greater than the client's so the difference is positive, but it might be negative if there was enough clock skew in that direction.

Clients can also query for tweets near other clients' locations, with a query like this:

WHATSAT kiwi.cs.ucla.edu 100 2
The arguments to a WHATSAT message are the name of another client (e.g., kiwi.cs.ucla.edu), a radius (in kilometers) from the client (e.g., 100), and an upper bound on the number of tweets to receive from Twitter senders within that radius of the client (e.g., 2). The upper bound must be at most 100, since that's all that Twitter supports conveniently.

The server responds with a AT message in the same format as before, giving the most recent location reported by the client, along with the server that it talked to and the time the server did the talking. Following the AT message is a JSON-format message, exactly in the same format that Twitter gives, followed by a newline. Here is an example (this output is line wrapped to print, but the actual output contains no newlines, except for the newline after the AT line, and the newline at the end):

AT Blake +0.563873386 kiwi.cs.ucla.edu +27.5916+086.5640 1353118103.108893381
{"results":[{"location":"Ever","profile_image_url":"http://a3.twimg.com/profile_images/524342107/avatar_normal.jpg","created_at":"Fri, 16 Nov 2012 07:38:34 +0000","from_user":"C_86","to_user_id":null,"text":"RT @ionmobile: @SteelCityHacker everywhere but nigeria // LMAO!","id":5704386230,"from_user_id":34011528,"geo":null,"iso_language_code":"en","source":"&lt;a href=&quot;http://socialscope.net&quot; rel=&quot;nofollow&quot;&gt;SocialScope&lt;/a&gt;"},{"location":"Ever","profile_image_url":"http://a3.twimg.com/profile_images/524342107/avatar_normal.jpg","created_at":"Fri, 16 Nov 2012 07:37:16 +0000","from_user":"C_86","to_user_id":null,"text":"RT @ionmobile: 25 minutes left! RT Who will win????? Follow @ionmobile","id":5704370354,"from_user_id":34011528,"geo":null,"iso_language_code":"en","source":"&lt;a href=&quot;http://socialscope.net&quot; rel=&quot;nofollow&quot;&gt;SocialScope&lt;/a&gt;"}],"max_id":5704386230,"since_id":5501341295,"refresh_url":"?since_id=5704386230&q=","next_page":"?page=2&max_id=5704386230&rpp=2&geocode=27.5916%2C86.564%2C100.0km&q=","results_per_page":2,"page":1,"completed_in":0.090181,"warning":"adjusted since_id to 5501341295 (2012-11-07 07:00:00 UTC), requested since_id was older than allowed -- since_id removed for pagination.","query":""}

Servers should respond to invalid commands with a line that contains a question mark (?), a space, and then a copy of the invalid command.

Servers communicate to each other too, using AT messages (or some variant of your design) to implement a simple flooding algorithm to propagate location updates to each other. Servers should not propagate tweets to each other, only locations; when asked for copies of tweets, a server should contact Twitter directly for them. Servers should continue to operate if their neighboring servers go down, that is, drop a connection and then reopen a connection later.

Each server should log its input and output into a file, using a format of your design. The logs should also contain notices of new and dropped connections from other servers. You can use the logs' data in your reports.

Write a report that summarizes your research, recommends whether Twisted is a suitable framework for this kind of application, and justifies your recommendation. Describe any problems you ran into. Your report should directly address your boss's worries about Python's type checking, memory management, and multithreading, compared to a Java-based approach to this problem. Your report should also briefly compare the overall approach of Twisted to that of Node.js, with the understanding that you probably won't have time to look deeply into Node.js before finishing this project.

Prepare and give a brief oral presentation of your research, and present it in discussion section. Please make sure your talk fits in the time allotted, as we don't want to cut you off abruptly.

Your research, report, and presentation should focus on language-related issues. For example, how easy is it to write Twisted-based programs that run and exploit server herds? What are the performance implications of using Twisted? Don't worry about nontechnical issues like licensing, or about management issues like software support and retraining programmers.

Style issues

Your report and talk are an important part of this assignment: they are not mere afterthoughts. Please see Resources for oral presentations and written reports for advice on generating high-quality reports and presentations.

Your report should use standard technical academic style, and should have a title, abstract, introduction, body, recommendations/conclusions, references, and any other sections necessary. Limit your report to at most five pages. Use the USENIX style, which uses a two-column format with 10-point font for most of the text, on an 8½"×11" page; an example of the output format and an example student paper are available.

Your paper is not expected to be just like that example student paper! That was written by a graduate student, she was writing a conference paper describing months of full-time research, and the paper is too long for us. It's merely an example of technical style and layout.

Research mechanics

If you need to run servers on SEASnet to do your research, please let the TA know how many TCP ports you need, and we will allocate them for you. Please do not use TCP ports at random, as that might collide with other students' uses.

You can grab a copy of the Twisted source code from the Twisted web site. You can also find a copy of the Twisted source code in the directory ~eggert/src/Twisted-12.2.0 on SEASnet; a distribution compressed tarball is in the file /u/cs/fac/eggert/src/tarpit/Twisted-12.2.0.tar.bz2.

You can run Twisted on SEASnet by prepending /usr/local/cs/bin to your PATH in the usual way. For example, you can run the chat server (taken from the Twisted code examples) as follows:

# Replace '12000' with your TCP port number as allocated by the TA.
sed 's/1025/12000/' /u/cs/fac/eggert/src/Twisted-12.2.0/doc/core/examples/chatserver.py >chatserver.py

# We recommend the -n option to forestall runaway servers.
twistd -n -y chatserver.py

# Now you can telnet to port 12000, from another session
# on the same host, to exercise the chat server.
# A log should appear on your screen.

# Type control-C to terminate the chat server.
If you can think of some similar project you'd like to do, that resembles the current project but is cooler, ask the T.A. for permission to do the similar project, and then go for it.

Submitting your work

Submit a file named report.pdf containing a copy of your paper in PDF form.

Submit a file named talk.pdf containing a copy of your presentation's visual aids in PDF form. Also submit a file containing the original format of the aids (talk.odp, talk.ppt, etc.), so that we can have them ready to go for your talk all using the same laptop, and not have to waste time connecting your laptop to the projector. Please consult with the TA well before the talk if your visual aids require special software (e.g., if you have a prototype server that you wish to demonstrate).

Submit any other supporting work (e.g., your source code) in a gzipped tar file named project.tgz.

Reference

Abe Fettig, Twisted network programming essentials, O'Reilly (October 2005), ISBN 0-596-10032-9. This is a Safari online book, so its contents should be viewable by anybody on the UCLA campus.

Kevin Makice, Twitter API: Up and Running: Learn How to Build Applications with the Twitter API, O'Reilly (March 2009), ISBN 978-0-596-15461-5.

Tim O'Reilly & Sarah Milstein, The Twitter Book, O'Reilly (May 2009), ISBN 978-0-596-80281-3.

© 2008–2012 Paul Eggert. See copying rules.
$Id: pr.html,v 1.37 2012/11/17 02:23:45 eggert Exp $