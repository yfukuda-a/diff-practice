jq -c '
  def bad_av:
    if type!="object" then true
    elif (keys|length)!=1 then true
    else
      (keys[0]) as $k
      | ( $k=="S" or $k=="N" or $k=="B" or $k=="BOOL" or $k=="NULL"
          or $k=="L" or $k=="M" or $k=="SS" or $k=="NS" or $k=="BS" ) as $known
      | if ($known|not) then true
        elif (has("SS") and (.SS|length==0))
          or (has("NS") and (.NS|length==0))
          or (has("BS") and (.BS|length==0)) then true
        else false end
    end;

  (.Items // .) as $all
  | range(0; ($all|length)) as $i
  | $all[$i] as $it
  | $it | to_entries[]
  | select(.value | bad_av)
  | {index:$i, attr:.key, value:.value}
' items.json
