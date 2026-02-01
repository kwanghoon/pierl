-module(pierl_demo).
-include("pierl.hrl").
-compile(export_all).

-define(PI, pierl).

%% Main =
%%   new printerChan.
%%   new forwarderChan.
%%   new ackChan.
%%   ( spawn Printer(printerChan)
%%     | spawn Forwarder(forwarderChan)
%%     | spawn Sender(senderChan, forwarderChan)
%%     | send forwarderChan<ackChan, printerChan>
%%     | delegate ackChan->senderChan )
%%
%% Forwarder(c) =
%%   recv c<ackChan, out>.(
%%     send ackChan<ack>
%%     | ForwarderLoop(c, out)
%%   )
%%
%% ForwarderLoop(c, out) =
%%   recv c<deleg chan>.(
%%     delegate chan->out
%%     | ForwarderLoop(c, out)
%%   )
%%   + recv c<m>.(
%%     send out<m>
%%     | ForwarderLoop(c, out)
%%   )
%%
%% Sender(self, forwarder) =
%%   recv self<deleg ackChan>.(
%%     recv ackChan<ack>.(
%%       new errorChan.
%%       delegate errorChan->forwarder
%%       | SenderBody(forwarder, errorChan, 2)
%%     )
%%   )
%%
%% SenderBody(print, err, 0) = send print<"done">
%% SenderBody(print, err, n) =
%%   ( send print<"remaining " ++ (n-1)>
%%     | SenderBody(print, err, n-1)
%%   )
%%   + ( send err<"failed">
%%       | SenderBody(print, err, n)
%%     )
%%
%% Printer(own) =
%%   recv own<deleg err>. PrinterLoop(own, err)
%%
%% PrinterLoop(print, err) =
%%   recv print<m>.(
%%     PrintSideEffect(m)
%%     | PrinterLoop(print, err)
%%   )
%%   + recv err<e>.(
%%     PrintErrorSideEffect(e)
%%     | PrinterLoop(print, err)
%%   )

forwarder(OwnChan) ->
    ?PI:recv(OwnChan, fun ({AckChan, OutputChan}) ->
        ?PI:send(AckChan, ack),
        forwarder(OwnChan, OutputChan)
    end).

forwarder(OwnChan, OutputChan) ->
    ?PI:recv(OwnChan, fun
            %% also forward delegations
            (?DELEGATION(Chan)) ->
                ?PI:delegate(Chan, OutputChan);
            (M) ->
                ?PI:send(OutputChan, M)
    end),
    forwarder(OwnChan, OutputChan).

sender(OwnChan, PrinterChan) ->
    random:seed(now()),
    ?PI:recv(OwnChan, fun (?DELEGATION(AckChan)) ->
        ?PI:recv(AckChan, fun (ack) ->
            %% got ack, we can go!
            ErrorChan = ?PI:new_chan(),
            ?PI:delegate(ErrorChan, PrinterChan),
            sender_body(PrinterChan, ErrorChan, 2)
            end
        )
    end).

sender_body(TargetChan, _, 0) ->
    ?PI:send(TargetChan,
       "Ok, I am done with you."
    );
sender_body(TargetChan, ErrorChan, Count) ->
    case random:uniform(3) of
        1 ->
            ?PI:send(TargetChan,
               "You will get this message " ++
                    integer_to_list(Count - 1) ++
                    " more times."
            ),
            sender_body(TargetChan, ErrorChan, Count - 1);
        _ ->
            ?PI:send(ErrorChan,
                "I failed to send you new messages, Printer."
            ),
            sender_body(TargetChan, ErrorChan, Count)
    end.

printer(OwnChan) ->
    ?PI:recv(OwnChan,
        fun (?DELEGATION(ErrorChan)) ->
            printer(OwnChan, ErrorChan)
        end
    ).

printer(PrintChan, ErrorChan) ->
    ?PI:recv([
        {PrintChan, fun (M) ->
            io:format(user,
                "Printer: oh hey, I got some message for printing: ~200p~n",
                [M]
             )
        end},
        {ErrorChan, fun (Err) ->
            io:format(user,
                "Printer: aahh! Something bad happened, err: ~200p~n",
                [Err]
             )
        end}
    ]),
    printer(PrintChan, ErrorChan).

start() ->
    ?PI:spawn(fun main/1).

main(_) ->
    random:seed(now()),
    PrinterChan = ?PI:spawn(fun printer/1),
    ForwarderChan = ?PI:spawn(fun forwarder/1),
    AckChan = ?PI:new_chan(),
    ?PI:send(ForwarderChan, {AckChan, PrinterChan}),
    SenderChan = ?PI:spawn(
        fun (SelfChan) -> sender(SelfChan, ForwarderChan) end),
    %% This so that sometimes the ack would end up in this process
    %% and sometimes it would get delayed and already arrive after the
    %% delegation.
    timer:sleep(random:uniform(12)),
    ?PI:delegate(AckChan, SenderChan).
