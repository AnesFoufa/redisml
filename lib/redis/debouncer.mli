type 'a t

val init :
  op:(unit -> 'a) ->
  ?miliseconds:int ->
  ?seconds:int ->
  ?minutes:int ->
  unit ->
  'a t

val debounce : 'a t -> 'a
