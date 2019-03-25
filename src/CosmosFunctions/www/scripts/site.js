'use strict';

(function () {
    var connection = null;
    var working = false;
    var initialized = false;
    Terminal.applyAddon(fit);
    Terminal.applyAddon(webLinks);
    var term = new Terminal({
        cursorBlink: true
    });
    var curr_line = "";

    var initialize = function () {
        term.writeln('Running set up routines...');
        $.get("/api/ConflictsDemoInitialize", {}, function () {
            $.get("/api/ConsistencyLatencyDemoInitialize", {}, function () {
                $.get("/api/CustomSynchronizationDemoInitialize", {}, function () {
                    $.get("/api/SingleMultiMasterDemoInitialize", {}, function () {
                        $.get("/api/SingleMultiRegionDemoInitialize", {}, function () {
                            initialized = true;
                            term.writeln('All containers ready, you can now run any of the demos.');
                            term.prompt();
                        });
                    });
                });
            }); 
        });
    };

    var callApi = function (apiName, title, description, after) {
        if (!initialized) {
            term.writeln('Before running any demo, please run the "init" command.');
            term.prompt();
            return;
        }

        working = true;
        term.writeln(title);
        if (description) {
            term.writeln(description);
        }

        term.writeln('-----------------------------------------');
        term.writeln('Executing...  ');
        if (description) {
            term.writeln("Running the scenario...");
        }

        $.get("/api/" + apiName, {}, function (data) {
            working = false;
            term.writeln('\b \b');
            term.writeln('Execution concluded.');

            if (Array.isArray(data)) {
                for (var i = 0; i < data.length; i++) {
                    term.writeln(data[i].test);
                    if (data[i].avgLatency && data[i].avgLatency > 0) {
                        term.writeln('* Avg. Latency: ' + data[i].avgLatency + ' ms');
                        term.writeln('* Average RU: ' + data[i].avgRU);
                    }
                }
            }

            if (after) {
                after();
            }

            term.prompt();
        });
    };

    var dispatch = function (command) {
        switch (command) {
            case "init":
                initialize();
                break;
            case "1":
                callApi("SingleMultiRegionDemo",
                    "Read latency between single region and multi-region replicated accounts",
                    "This test shows the difference in read latency for an account with a single master in SouthEast Asia region with a front end reading from it in West US 2. The next test shows the drastic improvement in latency with data locality when the account is replicated to West US 2.");
                break;
            case "2":
                callApi("ConsistencyLatencyDemo",
                    "Write latency for accounts with Eventual consistency vs. Strong consistency + impact of distance on Strong consistency",
                    "This test shows the difference in write latency for two accounts with replicas 1000 miles apart in West US 2 and Central US regions, one with Eventual consistency, the other with Strong consistency. There is a third test that shows the impact on latency when the distance between the regions is more than double the distance, demonstrating the speed of light impact on latency when using Strong consistency across large distances.");
                break;
            case "3":
                callApi("SingleMultiMasterDemo",
                    "Read and write latency for Single-Master account versus Multi-Master account",
                    "This test shows the difference in read latency for a single-master account (master: East US 2, replica: West US 2) with a client in West US 2. The next test shows the impact on write latency when using a multi-master account (master: East US 2, West US 2) with a client in West US 2.");
                break;
            case "4":
                callApi("ConflictsDemo",
                    "Multi-Master Conflict Resolution",
                    "This test shows the Last Write Wins and Merge Procedure conflict resolution modes as well as 'Async' mode where conflicts are written to the Conflicts Feed.",
                    function () {
                        term.writeln('Conflicts have been generated in the account, open the Azure Portal ( https://portal.azure.com ) to view them.');
                    });
                break;
            case "5":
                callApi("CustomSynchronizationDemo",
                    "Custom Synchronization",
                    "This test shows how to implement a custom synchronization between two regions. This allows you to have a lower level of consistency for a database with many replicas across great distances. This scenario shows an account with four regions (West US, West US 2, East US, East US 2) at Session level consistency but with Strong consistency between West US and West US 2. This provides for greater data durability (RPO = 0) without having to use Strong consistency across all regions and over very large distances. This demo includes a separate class that shows a simpler implementation of this you can more easily use without all the timer code.");
                break;
            case "cls":
                term.reset();
                term.prompt();
                break;
            case "help":
                help();
                term.prompt();
                break;
            default:
                term.writeln("Unrecognized command. Type help for more information.");
                term.prompt();
                break;
        }
    };

    var help = function () {
        term.writeln('Please select one among the available commands:');
        term.writeln(' init = Initialize required containers');
        term.writeln(' 1 = Single-Region vs. Multi-Region Read Latency');
        term.writeln(' 2 = Consistency vs. Latency');
        term.writeln(' 3 = Latency for Single-Master vs. Multi-Master');
        term.writeln(' 4 = Multi-Master Conflict Resolution');
        term.writeln(' 5 = Custom Synchronization');
        term.writeln(' cls = Clean the terminal');
        term.writeln(' help = This index');
    };

    term.open(document.getElementById('terminal'));
    term.fit();
    term.webLinksInit();

    window.onresize = function () {
        term.fit();
    };

    term.prompt = () => {
        term.write('\r\n$ ');
    };

    term.writeln('Welcome!');
    help();
    term.prompt();

    term.on('key', (key, ev) => {
        if (working) {
            return false;
        }

        var charCode = ev.which || ev.keyCode;
        const printable = !ev.altKey && !ev.altGraphKey && !ev.ctrlKey && !ev.metaKey && /[a-zA-Z0-9-_ ]/.test(String.fromCharCode(charCode));

        if (key.charCodeAt(0) === 13) {
            term.prompt();
            dispatch(curr_line);
            curr_line = "";
        } else if (charCode === 8) {
            // Do not delete the prompt
            if (curr_line.length > 0) {
                curr_line = curr_line.slice(0, -1);
                term.write('\b \b');
            }
        } else if (printable) {
            curr_line += ev.key;
            term.write(key);
        }
    });

    $.get("/api/getsignalrInfo", {}, function (data) {
        connection = new signalR.HubConnectionBuilder()
            .withUrl(data.url, { accessTokenFactory: () => data.accessToken })
            .build();
        connection.start();
        console.log(connection);
        connection.on('console', function (messages) {
            term.writeln(messages);
        });
    }, 'json');
})();