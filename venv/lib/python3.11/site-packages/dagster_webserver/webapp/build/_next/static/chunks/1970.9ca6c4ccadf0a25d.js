"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[1970],{91319:function(e,t,r){r.d(t,{y:function(){return p}});var n=r(52322),i=r(14934),s=r(35002),o=r(39013),l=r(36853),a=r(83373),d=r(47933),c=r(2728);let p=e=>{let{active:t=!1,item:r}=e,{type:o,icon:l,label:a,rightElement:d,tooltip:c="",disabled:p=!1}=r,u=(0,n.jsxs)(i.x,{padding:{vertical:4,left:12,right:8},flex:{direction:"row",gap:8,alignItems:"center",justifyContent:"space-between"},children:[(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8,alignItems:"center"},className:"iconAndLabel",children:[l,a]}),(0,n.jsx)("div",{children:d})]});return"link"!==o||p?(0,n.jsx)(s.u,{canShow:!!c,content:c,placement:"right",display:"block",children:(0,n.jsx)(x,{$active:t,disabled:p,onClick:r.onClick,children:u})}):(0,n.jsx)(s.u,{canShow:!!c,content:c,placement:"right",display:"block",children:(0,n.jsx)(h,{to:r.path,$active:t,children:u})})},u=(0,c.iv)(["background-color:",";border-radius:8px;color:",";display:block;font-size:14px;line-height:20px;text-decoration:none;transition:100ms background-color linear;user-select:none;width:100%;&:focus{outline:none;background-color:",";}&:hover,&:active{background-color:",";color:",";text-decoration:none;}.iconAndLabel{","{background-color:",";}}"],e=>{let{$active:t}=e;return t?o.Dd():"transparent"},e=>{let{$active:t}=e;return t?o.Ne():o.Ep()},e=>{let{$active:t}=e;return t?o.Dd():o.gJ()},e=>{let{$active:t}=e;return t?o.Dd():o._c()},e=>{let{$active:t}=e;return t?o.Ne():o.Ep()},l.a1,e=>{let{$active:t}=e;return t?o.Ne():o.Ep()}),h=(0,c.ZP)(d.rU).withConfig({componentId:"sc-14fe9a50-0"})(["",""],u),x=(0,c.ZP)(a.k).withConfig({componentId:"sc-14fe9a50-1"})(["",""],u)},14277:function(e,t,r){r.d(t,{c5:function(){return h}});var n=r(52322),i=r(60603),s=r(55010),o=r(38491),l=r(36853),a=r(92506),d=r(1747),c=r(20156),p=r(2784),u=r(45984);let h=e=>{let{locationNode:t}=e,[r,a]=(0,p.useState)(!1),[d,c]=(0,p.useState)(!1),u=null,h=null;return t.locationOrLoadError?.__typename==="RepositoryLocation"&&null!==t.locationOrLoadError.dagsterLibraryVersions&&(u=(0,n.jsx)(i.sN,{icon:"info",text:"View Dagster libraries",onClick:()=>c(!0)}),h=(0,n.jsx)(m,{libraries:t.locationOrLoadError.dagsterLibraryVersions,isOpen:d,setIsOpen:c})),(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(s.J,{position:"bottom-left",content:(0,n.jsxs)(i.v2,{children:[(0,n.jsx)(i.sN,{icon:"info",text:"View configuration",onClick:()=>a(!0)}),u]}),children:(0,n.jsx)(o.zx,{icon:(0,n.jsx)(l.JO,{name:"expand_more"})})}),(0,n.jsx)(x,{metadata:t.displayMetadata,isOpen:r,setIsOpen:a}),h]})},x=e=>{let{isOpen:t,setIsOpen:r,metadata:i}=e;return(0,n.jsxs)(a.Vq,{title:"Code location configuration",icon:"info",isOpen:t,onClose:()=>r(!1),style:{width:"600px"},children:[(0,n.jsx)(j,{displayMetadata:i}),(0,n.jsx)(a.cN,{topBorder:!0,children:(0,n.jsx)(o.zx,{onClick:()=>r(!1),intent:"primary",children:"Done"})})]})},m=e=>{let{isOpen:t,setIsOpen:r,libraries:i}=e;return(0,n.jsxs)(a.Vq,{title:"Dagster library versions",icon:"info",isOpen:t,onClose:()=>r(!1),style:{width:"600px"},children:[(0,n.jsxs)(d.i,{children:[(0,n.jsx)("thead",{children:(0,n.jsxs)("tr",{children:[(0,n.jsx)("th",{children:"Library"}),(0,n.jsx)("th",{children:"Version"})]})}),(0,n.jsx)("tbody",{children:i.map(e=>(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:e.name}),(0,n.jsx)("td",{children:e.version})]},e.name))})]}),(0,n.jsx)(a.cN,{topBorder:!0,children:(0,n.jsx)(o.zx,{onClick:()=>r(!1),intent:"primary",children:"Done"})})]})},j=e=>{let{displayMetadata:t}=e,r=(0,p.useMemo)(()=>{let e=t.reduce((e,t)=>(e[t.key]=t.value,e),{});return u.Pz(e)},[t]);return(0,n.jsx)(c.u,{value:r,options:{readOnly:!0,lineNumbers:!0,mode:"yaml"},theme:["config-editor"]})}},12576:function(e,t,r){r.d(t,{F8:function(){return $},OM:function(){return S},_y:function(){return N},hN:function(){return w}});var n=r(52322),i=r(35002),s=r(83373),o=r(53664),l=r(14934),a=r(39013),d=r(43212),c=r(89891),p=r(38491),u=r(36853),h=r(74188),x=r(2784),m=r(2728),j=r(81306),g=r(50309),f=r(85227),y=r(22203),b=r(68954);let S=e=>{let{metadata:t}=e,r=(0,f.m)(),l=t.find(e=>{let{key:t}=e;return"image"===t}),a=l?.value||"",d=(0,x.useCallback)(async()=>{r(a),await (0,g.B7)({intent:"success",icon:"done",message:"Image string copied!"})},[r,a]);return l?(0,n.jsxs)(v,{flex:{direction:"row",gap:4},children:[(0,n.jsx)("span",{style:{fontWeight:500},children:"image:"}),(0,n.jsx)(i.u,{content:"Click to copy",placement:"top",display:"block",children:(0,n.jsx)(s.k,{onClick:d,style:C,children:(0,n.jsx)(o.g,{text:l.value})})})]}):null},v=(0,m.ZP)(l.x).withConfig({componentId:"sc-5a4acf7-0"})(["width:100%;color:",";font-size:12px;.bp5-popover-target{overflow:hidden;}"],a.$()),$=e=>{let{metadata:t}=e,r=t.find(e=>{let{key:t}=e;return"module_name"===t||"package_name"===t||"python_file"===t});return r?(0,n.jsxs)(l.x,{flex:{direction:"row",gap:4},style:{width:"100%",color:a.$(),fontSize:12},children:[(0,n.jsxs)("span",{style:{fontWeight:500},children:[r.key,":"]}),(0,n.jsx)("div",{style:C,children:(0,n.jsx)(o.g,{text:r.value})})]}):null},N=e=>{let{locationStatus:t,locationOrError:r}=e,[i,s]=(0,x.useState)(!1),o=(0,x.useMemo)(()=>(0,b.je)(t?.name||""),[t?.name]),{reloading:a,tryReload:p}=(0,b.Dc)({scope:"location",reloadFn:o});return t?.loadStatus==="LOADING"?(0,n.jsx)(d.V,{minimal:!0,intent:"primary",children:"Updating…"}):r?.versionKey!==t?.versionKey?(0,n.jsx)(d.V,{minimal:!0,intent:"primary",children:"Loading…"}):t&&r?.locationOrLoadError?.__typename==="PythonError"?(0,n.jsxs)(n.Fragment,{children:[(0,n.jsxs)(l.x,{flex:{alignItems:"center",gap:12},children:[(0,n.jsx)(d.V,{minimal:!0,intent:"danger",children:"Failed"}),(0,n.jsx)(c.Z,{onClick:()=>s(!0),children:(0,n.jsx)("span",{style:{fontSize:"12px"},children:"View error"})})]}),(0,n.jsx)(j.p,{location:t.name,isOpen:i,error:r.locationOrLoadError,reloading:a,onDismiss:()=>s(!1),onTryReload:()=>p()})]}):(0,n.jsx)(d.V,{minimal:!0,intent:"success",children:"Loaded"})},w=e=>{let{location:t}=e;return(0,n.jsx)(y.s,{location:t,ChildComponent:e=>{let{reloading:t,tryReload:r,hasReloadPermission:s}=e;return(0,n.jsx)(l.x,{flex:{direction:"row",alignItems:"center",gap:4},children:(0,n.jsx)(i.u,{content:s?"":y.H,canShow:!s,useDisabledButtonTooltipFix:!0,children:(0,n.jsx)(p.zx,{icon:(0,n.jsx)(u.JO,{name:"code_location_reload"}),disabled:!s,loading:t,onClick:()=>r(),children:"Reload"})})})}})},C={width:"100%",display:"block",fontFamily:h.b.monospace,fontSize:"12px",color:a.$()}},51970:function(e,t,r){r.r(t),r.d(t,{WorkspaceRoot:function(){return nE},default:function(){return nk}});var n=r(52322),i=r(14934),s=r(37483),o=r(45774),l=r(23602),a=r(2784),d=r(7267),c=r(74580),p=r(35292),u=r(43212),h=r(20958),x=r(78491),m=r(69737),j=r(20171),g=r(79059),f=r(67126),y=r(59920),b=r(36381),S=r(59923),v=r(10539);let $=e=>{(0,m.Px)();let{repoAddress:t}=e,r=(0,d.UO)(),s=(0,b.p3)(r[0]),o=s.opNames.length>1?s.opNames[s.opNames.length-2]:s.pipelineName;return(0,j.j)(`Graph: ${o}`),(0,n.jsxs)("div",{style:{height:"100%",display:"flex",flexDirection:"column"},children:[(0,n.jsx)(c.m,{title:(0,n.jsx)(p.X6,{children:o}),tags:(0,n.jsxs)(u.V,{icon:"schema",children:["Graph in ",(0,n.jsx)(g.b,{repoAddress:t})]})}),(0,n.jsx)(i.x,{border:"top",style:{minHeight:0,flex:1,display:"flex"},children:(0,n.jsx)(N,{repoAddress:t})})]})},N=e=>{let{repoAddress:t}=e,r=(0,d.UO)(),i=(0,b.p3)(r["0"]),o=(0,d.k6)(),[l,c]=(0,a.useState)({explodeComposites:!1,preferAssetRendering:!0}),p=i.opNames.slice(0,i.opNames.length-1),u=(0,h.aM)(w,{variables:{graphSelector:{repositoryName:t?.name||"",repositoryLocationName:t?.location||"",graphName:i.pipelineName},rootHandleID:p.join("."),requestScopeHandleID:l.explodeComposites?void 0:p.join(".")}});return(0,n.jsx)(S.g,{queryResult:u,children:r=>{let{graphOrError:a}=r;if("GraphNotFoundError"===a.__typename)return(0,n.jsx)(s.t,{icon:"error",title:"Graph not found",description:a.message});if("PythonError"===a.__typename)return(0,n.jsx)(s.t,{icon:"error",title:"Query Error",description:a.message});let d=a.solidHandle,p=l.explodeComposites?(0,f.i)(a.solidHandles):a.solidHandles;return(0,n.jsx)(y.m0,{options:l,setOptions:c,explorerPath:i,onChangeExplorerPath:(t,r)=>{let n=(0,v.$U)(e.repoAddress,`/graphs/${(0,b.gY)(t)}`);"push"===r?o.push(n):o.replace(n)},container:a,repoAddress:t,handles:p,parentHandle:d||void 0,isGraph:!0,getInvocations:e=>p.filter(t=>t.solid.definition.name===e).map(e=>({handleID:e.handleID}))})}})},w=(0,h.Ps)`
  query GraphExplorerRootQuery(
    $graphSelector: GraphSelector
    $rootHandleID: String!
    $requestScopeHandleID: String
  ) {
    graphOrError(selector: $graphSelector) {
      ... on Graph {
        id
        name
        solidHandle(handleID: $rootHandleID) {
          ...GraphExplorerSolidHandleFragment
        }
        solidHandles(parentHandleID: $requestScopeHandleID) {
          handleID
          solid {
            name
          }
          ...GraphExplorerSolidHandleFragment
        }
        ...GraphExplorerFragment
      }
      ... on GraphNotFoundError {
        message
      }
      ...PythonErrorFragment
    }
  }

  ${y.Q}
  ${y.Sm}
  ${x.B}
`;var C=r(17867),E=r(22563),k=r(4199),A=r(17872),P=r(49308),O=r(73407),R=r(54044),_=r(6927),F=r(39268),T=r(35002),I=r(47933),U=r(71348);let D=()=>{let{paused:e}=(0,U.dx)();return(0,n.jsx)(T.u,{content:e?"Automation condition evaluation is paused. New materializations will not be triggered by automation conditions.":"",canShow:e,children:(0,n.jsx)(I.rU,{to:"/health",style:{outline:"none"},children:(0,n.jsx)(u.V,{icon:e?"toggle_off":"toggle_on",intent:e?"warning":"success",children:e?"Auto-materialize off":"Auto-materialize on"})})})};var M=r(75442),z=r(71469),L=r(44900),V=r(81535),J=r(89918),H=r(72447);let B=e=>{let{repoAddress:t,tab:r}=e;(0,m.Px)();let{groupName:s,0:o}=(0,d.UO)(),l=(0,d.k6)();(0,j.j)(`Asset Group: ${s}`);let u=(0,V.Q)(),h=(0,v.$U)(t,`/asset-groups/${s}`),x=(0,a.useMemo)(()=>({groupName:s,repositoryLocationName:t.location,repositoryName:t.name}),[s,t]),g=(0,a.useCallback)((e,t)=>{l[t]({pathname:`${h}/${(0,b.gY)(e)}`,search:l.location.search})},[h,l]),f=(0,a.useCallback)((e,t)=>{let r;r=t.groupName&&t.repoAddress?(0,v.$U)(t.repoAddress,`/asset-groups/${t.groupName}/lineage/${t.assetKey.path.map(encodeURIComponent).join("/")}`):(0,M.p)(t.assetKey,{view:"definition"}),e.metaKey?u(r):l.push(r)},[l,u]),y=a.useMemo(()=>({groupSelector:x,loading:!1}),[x]),S=a.useMemo(()=>({preferAssetRendering:!0,explodeComposites:!0}),[]);return(0,n.jsxs)(P.T,{style:{display:"flex",flexDirection:"column",paddingBottom:0},children:[(0,n.jsx)(c.m,{title:(0,n.jsx)(p.X6,{children:s}),right:(0,n.jsx)(H.i,{label:"Reload definitions"}),tags:(0,n.jsx)(Q,{groupSelector:x,repoAddress:t}),tabs:(0,n.jsxs)(i.x,{flex:{direction:"row",justifyContent:"space-between",alignItems:"center"},margin:{right:4},children:[(0,n.jsxs)(O.mQ,{selectedTabId:r,children:[(0,n.jsx)(J.f,{id:"lineage",title:"Lineage",to:`${h}/lineage`}),(0,n.jsx)(J.f,{id:"list",title:"List",to:`${h}/list`})]}),(0,n.jsx)(R.wP,{})]})}),"lineage"===r?(0,n.jsx)(z.OK,{fetchOptions:y,options:S,explorerPath:(0,b.p3)(o||"lineage/"),onChangeExplorerPath:g,onNavigateToSourceAssetNode:f,viewType:L._4.GROUP}):(0,n.jsx)(_.fv,{groupSelector:x,prefixPath:o.split("/").map(decodeURIComponent).filter(Boolean),setPrefixPath:e=>l.push(`${h}/list/${e.map(encodeURIComponent).join("/")}`)})]})},q=(0,h.Ps)`
  query AssetGroupMetadataQuery($selector: AssetGroupSelector!) {
    assetNodes(group: $selector) {
      id
      automationCondition {
        __typename
      }
    }
  }
`,Q=e=>{let{repoAddress:t,groupSelector:r}=e,i=(0,F.I)(),{data:s}=(0,h.aM)(q,{variables:{selector:r}});return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsxs)(u.V,{icon:"asset_group",children:["Asset Group in ",(0,n.jsx)(g.b,{repoAddress:t})]}),(()=>{let e=s?.assetNodes;return e&&0!==e.length&&"has-global-amp"===i&&e.some(e=>!!e.automationCondition)?(0,n.jsx)(D,{}):null})()]})};var K=r(38491);let G=e=>{let{repoAddress:t}=e;return(0,n.jsx)(p.X6,{children:(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8,alignItems:"center"},children:[(0,n.jsx)("div",{children:(0,n.jsx)(I.rU,{to:"/deployment/locations",children:"Code locations"})}),(0,n.jsx)("div",{children:"/"}),(0,n.jsx)("div",{children:(0,E.Uz)(t)})]})})};var W=r(14277),Z=r(12576);let Y=e=>{let{repoAddress:t}=e,{locationEntries:r,loading:i}=(0,a.useContext)(C.C5),s=r.find(e=>e.name===t.location);return(0,n.jsx)(c.m,{title:(0,n.jsx)(G,{repoAddress:t}),right:i||!s?null:(0,n.jsxs)(K.zH,{children:[(0,n.jsx)(Z.hN,{location:t.location}),(0,n.jsx)(W.c5,{locationNode:s})]})})},X=(e,t)=>e?.__typename!=="WorkspaceLocationEntry"||e.locationOrLoadError?.__typename!=="RepositoryLocation"?null:e.locationOrLoadError.repositories.find(e=>e.name===t.name)||null,ee=e=>{let{repoAddress:t,selectedTab:r,locationEntry:i}=e,s=(0,a.useMemo)(()=>X(i,t),[i,t]);return(0,n.jsxs)(O.mQ,{selectedTabId:r,children:[(0,n.jsx)(J.f,{id:"overview",title:"Overview",to:(0,v.$U)(t,"/")}),s?(0,n.jsx)(J.f,{id:"definitions",title:"Definitions",to:(0,v.$U)(t,"/definitions")}):(0,n.jsx)(O.OK,{id:"definitions",title:"Definitions",disabled:!0})]})};var et=r(74159),er=r(39013),en=r(36853),ei=r(88510),es=r(2728),eo=r(53664),el=r(17008);let ea=e=>{let{items:t,placeholder:r,nameFilter:o,renderRow:l}=e,[d,c]=(0,a.useState)(""),p=(0,a.useCallback)(e=>{c(e.target.value)},[]),u=d.trim().toLowerCase(),h=t.filter(e=>o(e,u)),x=(0,a.useRef)(null),m=(0,ei.MG)({count:h.length,getScrollElement:()=>x.current,estimateSize:()=>44,overscan:10}),j=m.getTotalSize(),g=m.getVirtualItems();return(0,n.jsxs)(i.x,{flex:{direction:"column"},style:{overflow:"hidden"},children:[(0,n.jsx)(i.x,{padding:{vertical:8,horizontal:24},children:(0,n.jsx)(et.oi,{value:d,onChange:p,placeholder:r,style:{width:"300px"},icon:"search"})}),(0,n.jsx)("div",{style:{flex:1,overflow:"hidden"},children:(0,n.jsxs)(el.W2,{ref:x,children:[(0,n.jsx)(el.VJ,{templateColumns:"1fr",sticky:!0,children:(0,n.jsx)(el.qN,{children:"Name"})}),g.length>0?(0,n.jsx)(el.Nh,{$totalHeight:j,children:g.map(e=>{let{index:t,key:r,size:i,start:s}=e,o=h[t];return(0,n.jsx)(el.X2,{$height:i,$start:s,children:l(o)},r)})}):(0,n.jsx)(i.x,{flex:{direction:"row",justifyContent:"center"},padding:{top:32},children:(0,n.jsx)(s.t,{icon:"search",title:"No matching results",description:(0,n.jsxs)(n.Fragment,{children:["No matching results for query ",(0,n.jsx)("strong",{children:d})," found in this code location."]})})})]})})]})},ed=e=>{let{iconName:t,label:r,path:s}=e;return(0,n.jsx)(i.x,{padding:{horizontal:24},border:"bottom",flex:{direction:"column",justifyContent:"center",alignItems:"flex-start"},style:{height:44,overflow:"hidden"},children:(0,n.jsx)(ec,{to:s,style:{width:"100%",overflow:"hidden"},children:(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8,alignItems:"center"},children:[(0,n.jsx)(en.JO,{name:t,color:er.Es()}),(0,n.jsx)("div",{style:{flex:1,overflow:"hidden"},children:(0,n.jsx)(eo.g,{text:r})})]})})})},ec=(0,es.ZP)(I.rU).withConfig({componentId:"sc-ccea7e94-0"})(["&:active,&:focus{outline:none;}"]),ep=e=>(0,a.useMemo)(()=>(e||"").replace(/(( ?> ?)|\.|\/)/g,"/").toLowerCase().trim(),[e]),eu=(e,t)=>{let r=ep(e);return(0,a.useMemo)(()=>r?t.filter(e=>(0,L.Je)("assetKey"in e?e.assetKey:e.key).toLowerCase().includes(r)):t,[t,r])};var eh=r(61083);let ex=(0,h.Ps)`
  fragment RepoAssetTableFragment on AssetNode {
    id
    assetKey {
      path
    }
    groupName
    ...AssetTableDefinitionFragment
  }

  ${eh.s}
`,em=(0,h.Ps)`
  query WorkspaceAssetsQuery($selector: RepositorySelector!) {
    repositoryOrError(repositorySelector: $selector) {
      ... on Repository {
        id
        name
        assetNodes {
          id
          ...RepoAssetTableFragment
        }
      }
      ...PythonErrorFragment
    }
  }

  ${ex}
  ${x.B}
`;var ej=r(20718),eg=r(71406),ef=r(4128),ey=r(30001);let eb=e=>Array.isArray(e)?e:[],eS=e=>{let{basePath:t}=(0,a.useContext)(ef.I),[r,n]=(0,ey.U)(`${t}:dagster.${e}`,eb),i=(0,a.useCallback)(e=>{n(t=>{let r=new Set(t||[]);return r.has(e)?r.delete(e):r.add(e),Array.from(r)})},[n]);return(0,a.useMemo)(()=>({expandedKeys:r,onToggle:i}),[r,i])},ev=e=>{let{repoAddress:t,assets:r}=e,n=(0,E.Uz)(t),{expandedKeys:i,onToggle:s}=eS(`${n}-assets-virtualized-expansion-state`),o=(0,a.useMemo)(()=>{let e={};for(let t of r){let r=t.groupName||"UNGROUPED";e[r]||(e[r]=[]),e[r].push(t)}return Object.values(e).forEach(e=>{e.sort((e,t)=>eg.Bl.compare((0,L.ll)(e.assetKey),(0,L.ll)(t.assetKey)))}),e},[r]);return{flattened:(0,a.useMemo)(()=>{let e=[];return Object.entries(o).sort((e,t)=>{let[r]=e,[n]=t;return eg.Bl.compare(r,n)}).forEach(t=>{let[r,n]=t;e.push({type:"group",name:r,assetCount:n.length}),i.includes(r)&&n.forEach(t=>{e.push({type:"asset",id:t.id,definition:t})})}),e},[o,i]),expandedKeys:new Set(i),onToggle:s}},e$=e=>{let{repoAddress:t}=e,[r,l]=(0,a.useState)(""),d=(0,E.Uz)(t),c=(0,ej.$)(t),{data:p,loading:u}=(0,h.aM)(em,{variables:{selector:c}}),x=eu(r,(0,a.useMemo)(()=>p?.repositoryOrError.__typename==="Repository"?p.repositoryOrError.assetNodes:[],[p])),{flattened:m,expandedKeys:j,onToggle:g}=ev({repoAddress:t,assets:x}),f=(0,a.useCallback)(e=>{l(e.target.value)},[]),y=(0,a.useRef)(null),b=(0,ei.MG)({count:m.length,getScrollElement:()=>y.current,estimateSize:()=>44,overscan:10}),S=b.getTotalSize(),v=b.getVirtualItems();return(0,n.jsxs)(i.x,{flex:{direction:"column"},style:{overflow:"hidden"},children:[(0,n.jsx)(i.x,{padding:{vertical:8,horizontal:24},children:(0,n.jsx)(et.oi,{value:r,onChange:f,placeholder:"Search assets by key…",style:{width:"300px"},icon:"search"})}),(0,n.jsx)("div",{style:{flex:1,overflow:"hidden"},children:u&&!p?(0,n.jsx)(i.x,{flex:{direction:"row",justifyContent:"center"},padding:32,children:(0,n.jsx)(o.f,{label:"Loading assets…"})}):x.length?(0,n.jsxs)(el.W2,{ref:y,children:[(0,n.jsx)(el.VJ,{templateColumns:"1fr",sticky:!0,children:(0,n.jsx)(el.qN,{children:"Name"})}),(0,n.jsx)(el.Nh,{$totalHeight:S,children:v.map(e=>{let{index:t,key:r,size:i,start:s}=e,o=m[t];if("group"===o.type)return(0,n.jsx)(eN,{height:i,start:s,expanded:j.has(o.name),groupName:o.name,assetCount:o.assetCount,onToggle:g},r);let{path:l}=o.definition.assetKey;return(0,n.jsx)(el.X2,{$height:i,$start:s,children:(0,n.jsx)(ed,{iconName:"asset",label:(0,L.ll)({path:l}),path:(0,M.p)({path:l})})},r)})})]}):r.trim().length>0?(0,n.jsx)(i.x,{padding:{top:20},children:(0,n.jsx)(s.t,{icon:"search",title:"No matching assets",description:(0,n.jsxs)("div",{children:["No assets matching ",(0,n.jsx)("strong",{children:r})," were found in ",d]})})}):(0,n.jsx)(i.x,{padding:{top:20},children:(0,n.jsx)(s.t,{icon:"search",title:"No assets",description:`No assets were found in ${d}`})})})]})},eN=e=>{let{groupName:t,assetCount:r,expanded:s,height:o,start:l,onToggle:a}=e;return(0,n.jsx)(ew,{$height:o,$start:l,onClick:()=>a(t),$open:s,tabIndex:0,onKeyDown:e=>{("Space"===e.code||"Enter"===e.code)&&(e.preventDefault(),a(t))},children:(0,n.jsxs)(i.x,{background:er.gJ(),flex:{direction:"row",alignItems:"center",gap:8,justifyContent:"space-between"},padding:{horizontal:24},border:"bottom",style:{height:"100%"},children:[(0,n.jsxs)(i.x,{flex:{alignItems:"center",gap:8},children:[(0,n.jsx)(en.JO,{name:"asset_group"}),"UNGROUPED"===t?(0,n.jsx)("div",{children:"Ungrouped assets"}):(0,n.jsx)("strong",{children:t})]}),(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:12},children:[(0,n.jsx)(u.V,{children:1===r?"1 asset":`${r} assets`}),(0,n.jsx)(en.JO,{name:"arrow_drop_down",size:20})]})]})})},ew=(0,es.ZP)(el.X2).withConfig({componentId:"sc-ec691413-0"})(["cursor:pointer;&:focus,&:active{outline:none;}","[aria-label='arrow_drop_down']{transition:transform 100ms linear;","}"],en.a1,e=>{let{$open:t}=e;return t?null:"transform: rotate(-90deg);"});var eC=r(99590);let eE=(0,h.Ps)`
  fragment RepositoryGraphsFragment on Repository {
    id
    usedSolids {
      definition {
        ... on CompositeSolidDefinition {
          id
          name
          description
        }
      }
      invocations {
        pipeline {
          id
          name
        }
        solidHandle {
          handleID
        }
      }
    }
    pipelines {
      id
      name
      isJob
      graphName
    }
  }
`,ek=(0,h.Ps)`
  query WorkspaceGraphsQuery($selector: RepositorySelector!) {
    repositoryOrError(repositorySelector: $selector) {
      ... on Repository {
        id
        ...RepositoryGraphsFragment
      }
      ...PythonErrorFragment
    }
  }

  ${eE}
  ${x.B}
`,eA=e=>{let t=Array.from(new Set(e.pipelines.filter(e=>e.isJob&&!(0,L.pv)(e.name)).map(e=>e.graphName))).map(e=>({name:e,path:`/graphs/${e}`,description:null}));return e.usedSolids.forEach(e=>{if("CompositeSolidDefinition"===e.definition.__typename){let r=e.invocations[0];r&&t.push({name:e.definition.name,path:`/graphs/${r.pipeline.name}/${r.solidHandle.handleID}/`,description:e.definition.description})}}),t.sort((e,t)=>eg.Bl.compare(e.name,t.name))},eP=e=>{let{repoAddress:t}=e,r=(0,ej.$)(t),{data:l,loading:d}=(0,h.aM)(ek,{variables:{selector:r}}),c=(0,a.useMemo)(()=>{let e=l?.repositoryOrError;return e&&"Repository"===e.__typename?eA(e):[]},[l]),p=(0,E.Uz)(t);return d?(0,n.jsx)(i.x,{padding:64,flex:{direction:"row",justifyContent:"center"},children:(0,n.jsx)(o.f,{label:"Loading graphs…"})}):l&&l.repositoryOrError?"PythonError"===l.repositoryOrError.__typename?(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(eC.T9,{error:l.repositoryOrError})}):"RepositoryNotFoundError"===l.repositoryOrError.__typename?(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"op",title:"Repository not found",description:`The repository ${p} could not be found in this workspace.`})}):c.length?(0,n.jsx)(ea,{items:c,placeholder:"Search graphs by name…",nameFilter:(e,t)=>e.name.toLowerCase().includes(t),renderRow:e=>(0,n.jsx)(ed,{iconName:"graph",label:e.name,path:(0,v.$U)(t,e.path)})}):(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"graph",title:"No graphs found",description:`The repository ${p} does not contain any graphs.`})}):(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"graph",title:"An unexpected error occurred",description:`An error occurred while loading graphs for ${p}`})})};var eO=r(49853),eR=r(89776),e_=r(19126),eF=r.n(e_),eT=r(41830),eI=r(56789);let eU=e=>{let{name:t,inputDefinitions:r,outputDefinitions:s}=e.definition,o=(0,eI.og)({name:t,inputs:r.map(e=>({definition:e,dependsOn:[]})),outputs:s.map(e=>({definition:e,dependedBy:[]})),definition:{description:null,assetNodes:[]}},{x:0,y:0});return(0,n.jsx)(i.x,{padding:24,children:(0,n.jsx)(eM,{style:{height:o.bounds.height},children:(0,n.jsx)(eT.Un,{invocation:void 0,definition:e.definition,minified:!1,onClick:()=>{},onDoubleClick:()=>{},onEnterComposite:()=>{},onHighlightEdges:()=>{},layout:o,selected:!1,focused:!1,highlightedEdges:[],dim:!1})})})},eD=(0,h.Ps)`
  fragment OpCardSolidDefinitionFragment on ISolidDefinition {
    name
    description
    metadata {
      key
      value
    }
    inputDefinitions {
      name
    }
    outputDefinitions {
      name
    }
    ...OpNodeDefinitionFragment
  }

  ${eT.WH}
`,eM=es.ZP.div.withConfig({componentId:"sc-edd60c99-0"})(["flex:1;max-width:450px;position:relative;"]);var ez=r(96266);let eL=e=>{let{name:t,onClickInvocation:r,repoAddress:i}=e,s=(0,ej.$)(i),o=(0,h.aM)(eV,{variables:{name:t,repositorySelector:s}});return(0,n.jsx)(S.g,{queryResult:o,children:e=>{let{repositoryOrError:t}=e;if(!(t?.__typename==="Repository"&&t.usedSolid))return null;let i=t.usedSolid;return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(eU,{definition:i.definition}),(0,n.jsx)(ez.I,{definition:i.definition,showingSubgraph:!1,onClickInvocation:r,getInvocations:()=>i.invocations.map(e=>({handleID:e.solidHandle.handleID,pipelineName:e.pipeline.name}))})]})}})},eV=(0,h.Ps)`
  query UsedSolidDetailsQuery($name: String!, $repositorySelector: RepositorySelector!) {
    repositoryOrError(repositorySelector: $repositorySelector) {
      ... on Repository {
        id
        usedSolid(name: $name) {
          definition {
            ...OpCardSolidDefinitionFragment
            ...SidebarOpDefinitionFragment
          }
          invocations {
            pipeline {
              id
              name
            }
            solidHandle {
              handleID
            }
          }
        }
      }
    }
  }

  ${eD}
  ${ez.u}
`,eJ=es.ZP.div.withConfig({componentId:"sc-7391293-0"})(["overflow-y:scroll;flex:1;"]);var eH=r(13233);function eB(e){let t={};for(let r of e)for(let e of r)t[e]=!0;return Object.keys(t).sort((e,t)=>e.localeCompare(t))}let eq=e=>{let{name:t,repoAddress:r,usedSolids:o}=e,l=(0,d.k6)(),c=(0,d.TH)(),{q:p,typeExplorer:u}=eF().parse(c.search,{ignoreQueryPrefix:!0}),h=[{token:"name",values:()=>o.map(e=>e.definition.name)},{token:"job",values:()=>eB(o.map(e=>e.invocations.filter(e=>!e.pipeline.isJob).map(e=>e.pipeline.name)))},{token:"pipeline",values:()=>eB(o.map(e=>e.invocations.filter(e=>e.pipeline.isJob).map(e=>e.pipeline.name)))},{token:"input",values:()=>eB(o.map(e=>e.definition.inputDefinitions.map(e=>e.type.displayName)))},{token:"output",values:()=>eB(o.map(e=>e.definition.outputDefinitions.map(e=>e.type.displayName)))}],x=(0,eO.HE)(p||"",h),m=o.filter(e=>{for(let t of x)if(("name"===t.token||void 0===t.token)&&!e.definition.name.startsWith(t.value)||("pipeline"===t.token||"job"===t.token)&&!e.invocations.some(e=>e.pipeline.name===t.value)||"input"===t.token&&!e.definition.inputDefinitions.some(e=>e.type.displayName.startsWith(t.value))||"output"===t.token&&!e.definition.outputDefinitions.some(e=>e.type.displayName.startsWith(t.value)))return!1;return!0}),j=(0,a.useMemo)(()=>[...m].sort((e,t)=>eg.Bl.compare(e.definition.name,t.definition.name)),[m]),g=o.find(e=>e.definition.name===t),f=e=>{l.replace({search:`?${eF().stringify({q:(0,eO.jP)(e)})}`})},y=e=>{l.replace((0,v.$U)(r,`/ops/${e}?${eF().stringify({q:p})}`))};a.useEffect(()=>{1!==j.length||g&&j[0]===g||y(j[0].definition.name),"string"==typeof u&&f([...x,{token:"input",value:u}])});let b=a.useCallback(e=>{let{pipelineName:t,handleID:n}=e;l.push((0,v.$U)(r,`/pipeline_or_job/${t}/${n.split(".").join("/")}`))},[l,r]);return(0,n.jsx)("div",{style:{height:"100%",display:"flex"},children:(0,n.jsx)(eR.O,{identifier:"ops",firstInitialPercent:40,firstMinSize:448,first:(0,n.jsxs)(eW,{children:[(0,n.jsx)(i.x,{padding:{vertical:12,horizontal:24},border:"bottom",children:(0,n.jsx)(eO.Gf,{values:x,onChange:e=>f(e),suggestionProviders:h,placeholder:"Filter by name or input/output type..."})}),(0,n.jsx)("div",{style:{flex:1,overflow:"hidden"},children:(0,n.jsx)(eQ,{selected:g,onClickOp:y,items:j})})]}),second:g?(0,n.jsx)(eJ,{children:(0,n.jsx)(eL,{name:g.definition.name,onClickInvocation:b,repoAddress:r})}):(0,n.jsx)(i.x,{padding:{vertical:64},children:(0,n.jsx)(s.t,{icon:"no-results",title:"No op selected",description:"Select an op to see its definition and invocations"})})})})},eQ=e=>{let{items:t,selected:r}=e,i=(0,a.useRef)(null),s=(0,ei.MG)({count:t.length,getScrollElement:()=>i.current,estimateSize:()=>42,overscan:10}),o=s.getTotalSize(),l=s.getVirtualItems(),d=r?t.findIndex(e=>e===r):void 0;return(0,n.jsx)(el.W2,{ref:i,children:(0,n.jsx)(el.Nh,{$totalHeight:o,children:l.map(r=>{let{index:i,size:s,start:o}=r,l=t[i];return(0,n.jsx)(el.X2,{$height:s,$start:o,children:(0,n.jsx)(eG,{$selected:d===i,onClick:()=>e.onClickOp(l.definition.name),children:(0,n.jsx)(eo.g,{text:l.definition.name})})},l.definition.name)})})})},eK=(0,h.Ps)`
  query OpsRootQuery($repositorySelector: RepositorySelector!) {
    repositoryOrError(repositorySelector: $repositorySelector) {
      ... on Repository {
        id
        usedSolids {
          ...OpsRootUsedSolid
        }
      }
      ...PythonErrorFragment
    }
  }

  fragment OpsRootUsedSolid on UsedSolid {
    definition {
      name
      ...OpTypeSignatureFragment
    }
    invocations {
      pipeline {
        id
        isJob
        name
      }
    }
  }

  ${eH.C}
  ${x.B}
`,eG=es.ZP.div.withConfig({componentId:"sc-e8e53872-0"})(["background:",";box-shadow:"," 4px 0 0 inset,"," 0 -1px 0 inset;color:",";cursor:pointer;font-size:14px;gap:8px;padding:12px 24px;user-select:none;overflow:hidden;white-space:nowrap;"],e=>{let{$selected:t}=e;return t?er.gJ():er.v8()},e=>{let{$selected:t}=e;return t?er.L$():"transparent"},er.wL(),e=>{let{$selected:t}=e;return t?er.Ep():er.$()}),eW=es.ZP.div.withConfig({componentId:"sc-e8e53872-1"})(["display:flex;flex-direction:column;height:100%;"]),eZ=e=>{let{repoAddress:t}=e,{name:r}=(0,d.UO)(),l=(0,ej.$)(t),{data:a,loading:c}=(0,h.aM)(eK,{variables:{repositorySelector:l}}),p=(0,E.Uz)(t);if(c)return(0,n.jsx)(i.x,{padding:64,flex:{direction:"row",justifyContent:"center"},children:(0,n.jsx)(o.f,{label:"Loading ops…"})});if(!a||!a.repositoryOrError)return(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"op",title:"An unexpected error occurred",description:`An error occurred while loading ops for ${p}`})});if("PythonError"===a.repositoryOrError.__typename)return(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(eC.T9,{error:a.repositoryOrError})});if("RepositoryNotFoundError"===a.repositoryOrError.__typename)return(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"op",title:"Repository not found",description:`The repository ${p} could not be found in this workspace.`})});let{repositoryOrError:u}=a,{usedSolids:x}=u;return x.length?(0,n.jsx)(eq,{name:r,repoAddress:t,usedSolids:x}):(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"op",title:"No ops found",description:`The repository ${(0,E.Uz)(t)} does not contain any ops.`})})},eY=e=>{let{repoAddress:t,repository:r}=e;return(0,n.jsx)(i.x,{flex:{direction:"column",alignItems:"stretch"},style:{flex:1,overflow:"hidden"},children:(0,n.jsxs)(d.rs,{children:[(0,n.jsx)(A.A,{path:"/locations/:repoPath/assets",children:(0,n.jsx)(e$,{repoAddress:t})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/jobs",children:(0,n.jsx)(eX,{repoAddress:t,repository:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/sensors",children:(0,n.jsx)(e0,{repoAddress:t,repository:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/schedules",children:(0,n.jsx)(e1,{repoAddress:t,repository:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/resources",children:(0,n.jsx)(e2,{repoAddress:t,repository:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/graphs",children:(0,n.jsx)(eP,{repoAddress:t})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/ops/:name?",children:(0,n.jsx)(eZ,{repoAddress:t})})]})})},eX=e=>{let{repoAddress:t,repository:r}=e,o=(0,a.useMemo)(()=>r.pipelines.filter(e=>{let{name:t}=e;return!(0,L.pv)(t)}).sort((e,t)=>eg.Bl.compare(e.name,t.name)),[r]);return o.length?(0,n.jsx)(ea,{items:o,placeholder:"Search jobs by name…",nameFilter:(e,t)=>e.name.toLowerCase().includes(t),renderRow:e=>(0,n.jsx)(ed,{iconName:"job",label:e.name,path:(0,v.$U)(t,`/jobs/${e.name}`)})}):(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"job",title:"No jobs found",description:`The repository ${(0,E.Uz)(t)} does not contain any jobs.`})})},e0=e=>{let{repoAddress:t,repository:r}=e,o=(0,a.useMemo)(()=>[...r.sensors].sort((e,t)=>eg.Bl.compare(e.name,t.name)),[r]);return o.length?(0,n.jsx)(ea,{items:o,placeholder:"Search sensors by name…",nameFilter:(e,t)=>e.name.toLowerCase().includes(t),renderRow:e=>(0,n.jsx)(ed,{iconName:"sensors",label:e.name,path:(0,v.$U)(t,`/sensors/${e.name}`)})}):(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"sensors",title:"No sensors found",description:`The repository ${(0,E.Uz)(t)} does not contain any sensors.`})})},e1=e=>{let{repoAddress:t,repository:r}=e,o=(0,a.useMemo)(()=>[...r.schedules].sort((e,t)=>eg.Bl.compare(e.name,t.name)),[r]);return o.length?(0,n.jsx)(ea,{items:o,placeholder:"Search schedules by name…",nameFilter:(e,t)=>e.name.toLowerCase().includes(t),renderRow:e=>(0,n.jsx)(ed,{iconName:"schedule",label:e.name,path:(0,v.$U)(t,`/schedules/${e.name}`)})}):(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"schedule",title:"No schedules found",description:`The repository ${(0,E.Uz)(t)} does not contain any schedules.`})})},e2=e=>{let{repoAddress:t,repository:r}=e,o=(0,a.useMemo)(()=>[...r.allTopLevelResourceDetails].sort((e,t)=>eg.Bl.compare(e.name,t.name)),[r]);return o.length?(0,n.jsx)(ea,{items:o,placeholder:"Search resources by name…",nameFilter:(e,t)=>e.name.toLowerCase().includes(t),renderRow:e=>(0,n.jsx)(ed,{iconName:"resource",label:e.name,path:(0,v.$U)(t,`/resources/${e.name}`)})}):(0,n.jsx)(i.x,{padding:64,children:(0,n.jsx)(s.t,{icon:"resource",title:"No resources found",description:`The repository ${(0,E.Uz)(t)} does not contain any resources.`})})};var e4=r(91319),e8=r(70812);let e3=e=>{let{repoAddress:t,repository:r}=e,{pathname:s}=(0,d.TH)(),o=r.assetGroups.length,l=r.pipelines.filter(e=>{let{name:t}=e;return!(0,L.pv)(t)}).length,a=r.schedules.length,c=r.sensors.length,p=r.allTopLevelResourceDetails.length,h=[{key:"assets",type:"link",icon:(0,n.jsx)(en.JO,{name:"asset"}),label:"Assets",path:(0,v.$U)(t,"/assets"),rightElement:o?(0,n.jsx)(u.V,{icon:"asset_group",children:e8.V.format(o)}):null},{key:"jobs",type:"link",icon:(0,n.jsx)(en.JO,{name:"job"}),label:"Jobs",path:(0,v.$U)(t,"/jobs"),rightElement:l?(0,n.jsx)(u.V,{children:e8.V.format(l)}):null},{key:"sensors",type:"link",icon:(0,n.jsx)(en.JO,{name:"sensors"}),label:"Sensors",path:(0,v.$U)(t,"/sensors"),rightElement:c?(0,n.jsx)(u.V,{children:e8.V.format(c)}):null},{key:"schedules",type:"link",icon:(0,n.jsx)(en.JO,{name:"schedule"}),label:"Schedules",path:(0,v.$U)(t,"/schedules"),rightElement:a?(0,n.jsx)(u.V,{children:e8.V.format(a)}):null},{key:"resources",type:"link",icon:(0,n.jsx)(en.JO,{name:"resource"}),label:"Resources",path:(0,v.$U)(t,"/resources"),rightElement:p?(0,n.jsx)(u.V,{children:e8.V.format(p)}):null},{key:"graphs",type:"link",icon:(0,n.jsx)(en.JO,{name:"graph"}),label:"Graphs",path:(0,v.$U)(t,"/graphs")},{key:"ops",type:"link",icon:(0,n.jsx)(en.JO,{name:"op"}),label:"Ops",path:(0,v.$U)(t,"/ops")}];return(0,n.jsx)(n.Fragment,{children:(0,n.jsx)(i.x,{padding:{bottom:12},children:h.map(e=>(0,n.jsx)(e4.y,{item:e,active:"link"===e.type&&s===e.path},e.key))})})},e6=e=>{let{repoAddress:t,repository:r}=e,{locationEntries:s,loading:o}=(0,a.useContext)(C.C5),l=s.find(e=>e.name===t.location);return l?(0,n.jsxs)(i.x,{style:{height:"100%",overflow:"hidden"},flex:{direction:"column"},children:[(0,n.jsx)(Y,{repoAddress:t}),(0,n.jsx)(i.x,{padding:{horizontal:24},border:"bottom",children:(0,n.jsx)(ee,{selectedTab:"definitions",repoAddress:t,locationEntry:l})}),(0,n.jsxs)(i.x,{style:{overflow:"hidden"},flex:{direction:"row",grow:1},children:[(0,n.jsx)(i.x,{style:{flex:"0 0 292px",overflowY:"auto"},padding:{vertical:16,horizontal:12},border:"right",children:(0,n.jsx)(e3,{repoAddress:t,repository:r})}),(0,n.jsx)(i.x,{flex:{direction:"column",alignItems:"stretch"},style:{flex:1,overflow:"hidden"},children:(0,n.jsx)(eY,{repoAddress:t,repository:r})})]})]}):o?(0,n.jsx)("div",{}):(0,n.jsx)(d.l_,{to:"/deployment/locations"})};var e9=r(1747),e5=r(74188),e7=r(20156);let te=e=>{let{locationName:t}=e;return null};var tt=r(45984);let tr=e=>{let{label:t,border:r=null}=e;return(0,n.jsx)(i.x,{background:er.gJ(),border:r,padding:{horizontal:24,vertical:8},children:(0,n.jsx)(p.pm,{children:t})})};var tn=r(35599),ti=r(81306),ts=r(68954);let to=e=>{let{repoAddress:t,locationEntry:r}=e,o=(0,E.Uz)(t),l=t.location,[d,c]=(0,a.useState)(!1),p=(0,a.useMemo)(()=>(0,ts.je)(l),[l]),{reloading:u,tryReload:h}=(0,ts.Dc)({scope:"location",reloadFn:p});return r?.locationOrLoadError?.__typename==="PythonError"?(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(s.t,{icon:"error_outline",title:"Error loading code location",description:(0,n.jsxs)(i.x,{flex:{direction:"column",gap:12},style:{wordBreak:"break-word"},children:[(0,n.jsxs)("div",{children:["Code location ",(0,n.jsx)("strong",{children:o})," failed to load due to errors."]}),(0,n.jsx)("div",{children:(0,n.jsx)(K.zx,{icon:(0,n.jsx)(en.JO,{name:"error_outline"}),onClick:()=>c(!0),children:"View errors"})})]})}),(0,n.jsx)(ti.p,{location:l,isOpen:d,error:r.locationOrLoadError,reloading:u,onDismiss:()=>c(!1),onTryReload:()=>h()})]}):(0,n.jsx)(s.t,{icon:"code_location",title:"Code location not found",description:(0,n.jsxs)(i.x,{flex:{direction:"column",gap:12},style:{wordBreak:"break-word"},children:[(0,n.jsxs)("div",{children:["Code location ",(0,n.jsx)("strong",{children:o})," is not available in this workspace."]}),(0,n.jsxs)("div",{children:["Check your ",(0,n.jsx)(I.rU,{to:"/deployment",children:"deployment settings"})," for errors."]})]})})},tl="280px",ta=e=>{let{repoAddress:t,locationStatus:r,locationEntry:s}=e,{displayMetadata:o}=s||{},l=(0,a.useMemo)(()=>({image:o?.find(e=>{let{key:t}=e;return"image"===t})||null}),[o]),d=(0,a.useMemo)(()=>tt.Pz(Object.fromEntries((o||[]).map(e=>{let{key:t,value:r}=e;return[t,r]}))),[o]),c=(0,a.useMemo)(()=>s?.locationOrLoadError?.__typename==="RepositoryLocation"?s?.locationOrLoadError.dagsterLibraryVersions:null,[s]);return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(i.x,{padding:{horizontal:24},border:"bottom",children:(0,n.jsx)(ee,{selectedTab:"overview",repoAddress:t,locationEntry:s})}),(0,n.jsx)(tr,{label:"Details"}),(0,n.jsx)(e9.i,{style:{width:"100%",tableLayout:"fixed"},children:(0,n.jsxs)("tbody",{children:[(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{style:{width:tl,minWidth:tl,verticalAlign:"middle"},children:"Status"}),(0,n.jsx)("td",{children:(0,n.jsx)(Z._y,{locationStatus:r,locationOrError:s})})]}),(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Updated"}),(0,n.jsx)("td",{children:r?(0,n.jsx)("div",{style:{whiteSpace:"nowrap"},children:(0,n.jsx)(tn.C,{unixTimestamp:r.updateTimestamp})}):null})]}),l.image?(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Image"}),(0,n.jsx)("td",{style:{fontFamily:e5.b.monospace},children:(0,n.jsx)(eo.g,{text:l.image.value})})]}):null]})}),(0,n.jsx)(te,{locationName:t.location}),c?.length?(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(tr,{label:"Libraries"}),(0,n.jsx)(e9.i,{children:(0,n.jsx)("tbody",{children:c.map(e=>(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{style:{width:tl},children:(0,n.jsx)(p.fv,{children:e.name})}),(0,n.jsx)("td",{children:(0,n.jsx)(p.fv,{children:e.version})})]},e.name))})})]}):null,(0,n.jsx)(tr,{label:"Metadata",border:"bottom"}),(0,n.jsx)(tc,{}),(0,n.jsx)("div",{style:{height:"320px"},children:(0,n.jsx)(e7.u,{options:{readOnly:!0,lineNumbers:!1},theme:["code-location-metadata"],value:d})})]})};var td=e=>{let{repoAddress:t}=e,{locationEntries:r,locationStatuses:s,loading:l}=(0,a.useContext)(C.C5),d=r.find(e=>e.name===t.location),c=s[t.location];return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(Y,{repoAddress:t}),(()=>{if(!d||!c){let e=(0,E.Uz)(t);if(l)return(0,n.jsx)(i.x,{padding:64,flex:{direction:"row",justifyContent:"center"},children:(0,n.jsx)(o.f,{label:`Loading ${e}…`})});if(!d&&!c)return(0,n.jsx)(i.x,{padding:64,flex:{direction:"row",justifyContent:"center"},children:(0,n.jsx)(to,{repoAddress:t,locationEntry:d||null})})}return(0,n.jsx)(ta,{repoAddress:t,locationEntry:d||null,locationStatus:c||null})})()]})};let tc=(0,es.vJ)([".CodeMirror.cm-s-code-location-metadata.cm-s-code-location-metadata{background-color:",";padding:12px 20px;height:300px;}"],er.v8());var tp=r(99478),tu=r(76016),th=r(92199);let tx=e=>{(0,m.Px)();let{repoAddress:t}=e,r=(0,d.k6)(),i=(0,d.TH)(),s=(0,d.UO)()["0"],o=(0,a.useMemo)(()=>(0,b.p3)(s),[s]),l=(0,V.Q)(),c=(0,th.Ux)(t),p=(0,th.E8)(c,o.pipelineName);(0,tu.b)(o,p),(0,b.Bj)({pipelinePath:(0,b.gY)(o)});let u=(0,a.useCallback)((e,n)=>{r[n]({search:i.search,pathname:(0,v.$U)(t,`/${p?"jobs":"pipelines"}/${(0,b.gY)(e)}`)})},[r,i.search,t,p]),h=(0,a.useCallback)((e,t)=>{if(!t.jobName||!t.opNames.length||!t.repoAddress){let n=(0,M.p)(t.assetKey,{view:"definition"});e.metaKey?l(n):r.push(n);return}r.replace({search:i.search,pathname:(0,v.$U)(t.repoAddress,`/jobs/${(0,b.gY)({...o,opNames:[(0,L.Je)(t.assetKey)],opsQuery:"",pipelineName:t.jobName})}`)})},[o,r,i.search,l]);return(0,n.jsx)(tp.N9,{repoAddress:t,explorerPath:o,onChangeExplorerPath:u,onNavigateToSourceAssetNode:h})},tm=e=>{let{repoAddress:t}=e;return(0,n.jsx)(tx,{repoAddress:t})};var tj=r(36803),tg=r(34219);let tf=e=>{let{repoAddress:t}=e,r=(0,d.TH)(),{pipelinePath:i}=(0,d.UO)(),{loading:s}=(0,a.useContext)(C.C5),{loading:o}=(0,a.useContext)(tj.IT),l=(0,th.Ux)(t);if((0,tg.j$)("Workspace",s),(0,tg.j$)("Permissions",o),s||o)return null;let{pipelineName:c}=(0,b.p3)(i),p=(0,th.E8)(l,c),{pathname:u,search:h}=r,x=u.replace("/pipeline_or_job/",p?"/jobs/":"/pipelines/");return(0,n.jsx)(d.l_,{to:`${x}${h}`})};var ty=r(29664),tb=r(46352),tS=r(62233),tv=r(28025),t$=r(1040);let tN=e=>{let{repoAddress:t}=e,{permissions:{canLaunchPipelineExecution:r},loading:i}=(0,tj.xz)(t.location),{repoPath:s,pipelinePath:o,runId:l}=(0,d.UO)();return((0,tg.j$)("Permissions",i),i)?null:r?(0,n.jsx)(tw,{pipelinePath:o,repoAddress:t,runId:l}):(0,n.jsx)(d.l_,{to:`/locations/${s}/pipeline_or_job/${o}`})},tw=e=>{let{pipelinePath:t,repoAddress:r,runId:i}=e,s=(0,b.p3)(t),{pipelineName:o}=s,l=(0,th.Ux)(r),a=(0,th.E8)(l,o);(0,tu.b)(s,a);let[c,p]=(0,t$.aC)(r,o),{data:u,loading:x}=(0,h.aM)(tC,{variables:{runId:i},onCompleted:e=>{let t=e?.runOrError,r=t?.__typename==="Run"?t:null;if(!r)return;let{runConfigYaml:n,mode:i,solidSelection:s}=r;if(!n&&!i&&!s)return;let o={name:`From run ${r.id.slice(0,8)}`};"string"==typeof n&&(o.runConfigYaml=n),"string"==typeof i&&(o.mode=i);let l=null;s instanceof Array&&s.length>0?l=s:"string"==typeof s&&s&&(l=[s]),o.solidSelection=l,o.solidSelectionQuery=l?l.join(","):"*",p(e=>(0,t$.f0)(e,o))}}),m=u?.runOrError;return x?(0,n.jsx)(tv.m,{}):m&&"RunNotFoundError"!==m.__typename?"PythonError"===m.__typename?(0,n.jsx)(tS.U,{icon:"error",title:"Python error",description:m.message}):(0,n.jsx)(d.l_,{to:{pathname:(0,v.$U)(r,`/${a?"jobs":"pipelines"}/${o}/playground`)}}):(0,n.jsx)(tS.U,{icon:"error",title:"No run found",description:"The run with this ID does not exist or has been cleaned up."})},tC=(0,h.Ps)`
  query ConfigForRunQuery($runId: ID!) {
    runOrError(runId: $runId) {
      ... on Run {
        id
        mode
        runConfigYaml
        solidSelection
      }
      ...PythonErrorFragment
    }
  }

  ${x.B}
`,tE=e=>{let{repoAddress:t}=e,{permissions:{canLaunchPipelineExecution:r},loading:i}=(0,tj.xz)(t.location);(0,tg.j$)("Permissions",i);let{repoPath:s,pipelinePath:o}=(0,d.UO)();return i?null:r?(0,n.jsx)(tk,{pipelinePath:o,repoAddress:t}):(0,n.jsx)(d.l_,{to:`/locations/${s}/pipeline_or_job/${o}`})},tk=e=>{let{pipelinePath:t,repoAddress:r}=e,i=(0,b.p3)(t),{pipelineName:s}=i,o=(0,th.Ux)(r),l=(0,th.E8)(o,s);(0,tu.b)(i,l);let[c,p]=(0,t$.aC)(r,s),u=eF().parse(window.location.search,{ignoreQueryPrefix:!0});return(0,a.useEffect)(()=>{if(u.config||u.mode||u.solidSelection||u.tags||u.assetSelection){let e={};"string"==typeof u.config&&(e.runConfigYaml=u.config),"string"==typeof u.mode&&(e.mode=u.mode),u.solidSelection instanceof Array&&u.solidSelection.length>0?e.solidSelection=u.solidSelection:"string"==typeof u.solidSelection&&u.solidSelection&&(e.solidSelection=[u.solidSelection]),"string"==typeof u.solidSelectionQuery&&(e.solidSelectionQuery=u.solidSelectionQuery),Array.isArray(u.tags)&&(e.tags=u.tags),Array.isArray(u.assetSelection)&&(e.assetSelection=u.assetSelection),p(t=>(0,t$.f0)(t,e))}}),(0,n.jsx)(d.l_,{to:{pathname:(0,v.$U)(r,`/${l?"jobs":"pipelines"}/${s}/playground`)}})},tA=["overview","playground","runs","partitions"],tP=e=>{let{repoAddress:t,isJob:r,explorerPath:i,matchingTab:s="",permissions:o,tabs:l}=e,d=(0,b.gY)({...i,opNames:[]}),c=(0,a.useMemo)(()=>l.find(e=>e.pathComponent===s)||l.find(e=>""===e.pathComponent),[s,l]);return(0,n.jsx)(O.mQ,{size:"large",selectedTabId:c.id,children:l.filter(e=>!e.isHidden).map(e=>{let{id:i,title:s,getPermissionsResult:l}=e,a=l?l(o):null,c=!!(a&&!a.enabled),p=a&&c?(0,n.jsx)(T.u,{content:a.disabledReason,placement:"top",children:s}):s,u=(0,v.$U)(t,`/${r?"jobs":"pipelines"}/${d}${e.pathComponent}`);return c?(0,n.jsx)(O.OK,{disabled:!0,id:i,title:p},i):(0,n.jsx)(J.f,{id:i,title:p,disabled:c,to:u},i)})})},tO=e=>{let{hasLaunchpad:t,hasPartitionSet:r}=e;return{overview:{id:"overview",title:"Overview",pathComponent:""},playground:{id:"launchpad",title:"Launchpad",pathComponent:"playground",getPermissionsResult:e=>(0,tj.mr)(e,"canLaunchPipelineExecution"),isHidden:!t},runs:{id:"runs",title:"Runs",pathComponent:"runs"},partitions:{id:"partitions",title:"Partitions",pathComponent:"partitions",isHidden:!r}}},tR=e=>{let t=tO(e);return tA.map(e=>t[e]).filter(e=>!!e&&!e.isHidden)};var t_=r(89891),tF=r(92506),tT=r(75652),tI=r.n(tT),tU=r(90441),tD=r(16336),tM=r(88257),tz=r(54516),tL=r(43984),tV=r(68283),tJ=r(33182),tH=r(86269);let tB={showSeconds:!0,showTimezone:!1},tq=e=>{let{pipelineName:t,repoAddress:r}=e,s=(0,h.aM)(tQ,{variables:{runsFilter:{pipelineName:t,tags:[{key:tL.H.RepositoryLabelTag,value:(0,E.Wg)(r)}]}},notifyOnNetworkStatusChange:!0});(0,tD.C4)(s,tD.dT);let o=(0,a.useMemo)(()=>{let e=s.data?.pipelineRunsOrError;return e&&"Runs"===e.__typename&&e.results[0]||null},[s]);if(!o)return null;let l={start:o.startTime,end:o.endTime,status:o.status};return(0,n.jsx)(u.V,{intent:(()=>{switch(o.status){case tM.VOA.SUCCESS:return"success";case tM.VOA.CANCELED:case tM.VOA.CANCELING:case tM.VOA.FAILURE:return"danger";default:return"none"}})(),children:(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:4},children:[(0,n.jsx)(tz.K9,{status:o.status,size:10}),"Latest run:",l?(0,n.jsx)(T.u,{placement:"bottom",content:(0,n.jsx)(tU.k6,{children:(0,n.jsxs)("tbody",{children:[(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{style:{color:er.Aw()},children:(0,n.jsx)(i.x,{padding:{right:16},children:"Started"})}),(0,n.jsx)("td",{children:l.start?(0,n.jsx)(tH.v,{timestamp:l.start,timeFormat:tB}):(0,tV.Z0)(l.status)})]}),(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{style:{color:er.Aw()},children:"Ended"}),(0,n.jsx)("td",{children:l.end?(0,n.jsx)(tH.v,{timestamp:l.end,timeFormat:tB}):(0,tV.Z0)(l.status)})]})]})}),children:(0,n.jsx)(I.rU,{to:`/runs/${o.id}`,children:(0,n.jsx)(tJ.Zr,{run:o})})}):null]})})},tQ=(0,h.Ps)`
  query LatestRunTagQuery($runsFilter: RunsFilter) {
    pipelineRunsOrError(filter: $runsFilter, limit: 1) {
      ... on Runs {
        results {
          id
          status
          ...RunTimeFragment
        }
      }
    }
  }

  ${tJ.$X}
`;var tK=r(74283),tG=r(4219),tW=r(60918);let tZ=e=>{let{pipelineName:t,repoAddress:r}=e,i=function(e,t){let r=(0,h.aM)(t0,{variables:{runsFilter:{pipelineName:t,tags:[{key:tL.H.RepositoryLabelTag,value:(0,E.Wg)(e)}]},params:{pipelineName:t,repositoryName:e.name,repositoryLocationName:e.location}}}).data;return(0,a.useMemo)(()=>({assetNodes:r?.assetNodes||null,job:r?.pipelineOrError&&"Pipeline"===r.pipelineOrError.__typename?r.pipelineOrError:null,runsForAssetScan:r?.pipelineRunsOrError&&"Runs"===r.pipelineRunsOrError.__typename?r.pipelineRunsOrError.results:[]}),[r])}(r,t);return(0,n.jsxs)(n.Fragment,{children:[i.job?(0,n.jsx)(tY,{job:i.job,repoAddress:r}):null,(0,n.jsx)(tq,{pipelineName:t,repoAddress:r}),i.assetNodes&&i.assetNodes.some(e=>!!e.automationCondition)&&(0,n.jsx)(D,{}),i.runsForAssetScan?(0,n.jsx)(tX,{relatedAssets:i.assetNodes?i.assetNodes.map(e=>(0,L.Je)(e.assetKey)):tI()(i.runsForAssetScan.flatMap(e=>e.assets.map(e=>(0,L.Je)(e.key))))}):null]})},tY=e=>{let{job:t,repoAddress:r}=e,i=(0,a.useMemo)(()=>t?.__typename==="Pipeline"&&t.schedules.length?t.schedules:[],[t]),s=(0,a.useMemo)(()=>t?.__typename==="Pipeline"&&t.sensors.length?t.sensors:[],[t]);return(0,n.jsx)(tK.I,{schedules:i,sensors:s,repoAddress:r})},tX=e=>{let{relatedAssets:t}=e,[r,s]=(0,a.useState)(!1);if(0===t.length)return null;if(1===t.length){let e=t[0];return(0,n.jsxs)(u.V,{icon:"asset",children:["Asset: ",(0,n.jsx)(I.rU,{to:`/assets/${e}`,children:e})]})}return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(u.V,{icon:"asset",children:(0,n.jsx)(t_.Z,{color:er.Es(),onClick:()=>s(!0),children:`View ${t.length} assets`})}),(0,n.jsxs)(tF.Vq,{title:"Related assets",canOutsideClickClose:!0,canEscapeKeyClose:!0,isOpen:r,onClose:()=>s(!1),style:{maxWidth:"80%",minWidth:"500px",width:"auto"},children:[t.map((e,r)=>(0,n.jsx)(i.x,{padding:{vertical:12,horizontal:20},border:r<t.length-1?"bottom":null,children:(0,n.jsx)(I.rU,{to:`/assets/${e}`,style:{wordBreak:"break-word"},children:e},e)},e)),(0,n.jsx)(tF.cN,{topBorder:!0,children:(0,n.jsx)(K.zx,{intent:"primary",onClick:()=>s(!1),children:"OK"})})]})]})},t0=(0,h.Ps)`
  query JobMetadataQuery($params: PipelineSelector!, $runsFilter: RunsFilter!) {
    pipelineOrError(params: $params) {
      ... on Pipeline {
        id
        ...JobMetadataFragment
      }
    }
    assetNodes(pipeline: $params) {
      id
      ...JobMetadataAssetNode
    }
    pipelineRunsOrError(filter: $runsFilter, limit: 5) {
      ... on PipelineRuns {
        results {
          id
          ...RunMetadataFragment
        }
      }
    }
  }

  fragment JobMetadataAssetNode on AssetNode {
    id
    automationCondition {
      __typename
    }
    assetKey {
      path
    }
  }

  fragment JobMetadataFragment on Pipeline {
    id
    isJob
    name
    schedules {
      id
      mode
      ...ScheduleSwitchFragment
    }
    sensors {
      id
      targets {
        pipelineName
        mode
      }
      ...SensorSwitchFragment
    }
  }

  fragment RunMetadataFragment on PipelineRun {
    id
    status
    assets {
      id
      key {
        path
      }
    }
    ...RunTimeFragment
  }

  ${tG.J}
  ${tW.U}
  ${tJ.$X}
`,t1=e=>{let{repoAddress:t}=e,r=(0,tj.xz)(t.location),s=(0,d.$B)(["/locations/:repoPath/pipelines/:selector/:tab?","/locations/:repoPath/jobs/:selector/:tab?","/locations/:repoPath/pipeline_or_job/:selector/:tab?"]),o=(0,b.p3)(s.params.selector),{pipelineName:l,snapshotId:a}=o,h=(0,th.Ux)(t),x=h?.repository.pipelines.find(e=>e.name===l),m=!!x?.isJob,j=tR({hasLaunchpad:!x?.isAssetJob,hasPartitionSet:(h?.repository.partitionSets||[]).some(e=>e.pipelineName===l)});return(0,n.jsx)(n.Fragment,{children:(0,n.jsx)(c.m,{title:(0,n.jsxs)(p.X6,{style:{display:"flex",flexDirection:"row",gap:4},children:[(0,n.jsx)(I.rU,{to:"/jobs",children:"Jobs"}),(0,n.jsx)("span",{children:"/"}),l]}),tags:(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:8,wrap:"wrap"},children:[(0,n.jsxs)(u.V,{icon:"job",children:[m?"Job in ":"Pipeline in ",(0,n.jsx)(g.b,{repoAddress:t})]}),a?null:(0,n.jsx)(tZ,{pipelineName:l,repoAddress:t})]}),tabs:(0,n.jsx)(tP,{repoAddress:t,isJob:m,explorerPath:o,permissions:r,matchingTab:s.params.tab,tabs:j})})})};var t2=r(60129),t4=r(80122),t8=r(90731);let t3=e=>{let{partitionSetName:t,partitionNames:r,repositorySelector:o,refetchCounter:l}=e,[d,c]=(0,a.useState)(()=>[]),[p,u]=(0,a.useState)(),x=(0,h.aM)(t6,{variables:{partitionSetName:t,repositorySelector:o,cursor:p,limit:10}}),m=x.refetch;return(0,a.useEffect)(()=>{l&&m()},[m,l]),(0,n.jsx)(S.g,{queryResult:x,children:e=>{let{partitionSetOrError:t}=e;if("PartitionSetNotFoundError"===t.__typename)return(0,n.jsx)(i.x,{margin:{vertical:20},children:(0,n.jsx)(s.t,{title:"Partition set not found.",icon:"no-results"})});if("PythonError"===t.__typename)return(0,n.jsx)(i.x,{margin:{vertical:20},children:(0,n.jsx)(s.t,{title:"An error occurred.",icon:"no-results"})});let{backfills:o,pipelineName:l}=t;if(!o.length)return(0,n.jsx)(i.x,{margin:{vertical:20},children:(0,n.jsx)(s.t,{title:`No backfills for ${l}`,icon:"no-results"})});let a={hasPrevCursor:!!p,hasNextCursor:o&&10===o.length,popCursor:()=>{let e=[...d];u(e.pop()),c(e)},advanceCursor:()=>{p&&c(e=>[...e,p]);let e=o&&o[o.length-1].id;e&&u(e)},reset:()=>{c([]),u(void 0)}};return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(t8.GQ,{backfills:o,refetch:m,showBackfillTarget:!1,allPartitions:r}),(0,n.jsx)(t4.b$,{...a})]})}})},t6=(0,h.Ps)`
  query JobBackfillsQuery(
    $partitionSetName: String!
    $repositorySelector: RepositorySelector!
    $cursor: String
    $limit: Int
  ) {
    partitionSetOrError(
      repositorySelector: $repositorySelector
      partitionSetName: $partitionSetName
    ) {
      ... on PartitionSet {
        id
        pipelineName
        backfills(cursor: $cursor, limit: $limit) {
          id
          ...BackfillTableFragment
        }
      }
    }
  }

  ${t8.be}
`;var t9=r(79106),t5=r(37504),t7=r(54048),re=r(90086),rt=r(67994),rr=r(77851);let rn=e=>{let{counts:t,value:r,onChange:s,allowed:o,disabled:l}=e;return(0,n.jsx)(i.x,{flex:{direction:"row",alignItems:"center",gap:12},style:{overflow:"hidden"},children:o.map(e=>(0,n.jsx)(t5.X,{"data-testid":(0,rr.N)(`run-status-${e}-checkbox`),disabled:l,style:{marginBottom:0,marginLeft:10,minWidth:200},checked:r.includes(e)&&!l,label:`${(0,rt.fi)(e)} (${t[e]})`,onChange:()=>s(r.includes(e)?r.filter(t=>t!==e):[...r,e])},e))})};var ri=r(98832),rs=r(97803),ro=r(95166),rl=r(77432),ra=r(57755),rd=r(97145),rc=r(45546),rp=r(59867);let ru=e=>{let{partitionSetName:t,onLaunch:r,onCancel:o,onSubmit:l,repoAddress:c,runStatusData:u,refreshing:x,pipelineName:m,partitionNames:j}=e,g=(0,d.k6)(),[y,b]=a.useState(Object.keys(u).filter(e=>!u[e]||u[e]===tM.VOA.FAILURE)),[S,v]=a.useState([tM.VOA.NOT_STARTED,tM.VOA.FAILURE]),$=a.useMemo(()=>y.filter(e=>S.includes(u[e])),[y,S,u]),[N,w]=a.useState(!1),[C,E]=a.useState([]),[k,A]=a.useState(""),[P,O]=a.useState({reexecute:!1,fromFailure:!1}),R=(0,ej.$)(c),{data:_}=(0,h.aM)(rx,{variables:{pipelineSelector:{...R,pipelineName:m}},fetchPolicy:"network-only"}),F=a.useRef(!0);if(a.useEffect(()=>(F.current=!0,()=>{F.current=!1}),[r]),!_)return(0,n.jsx)(i.x,{margin:{vertical:32},flex:{justifyContent:"center"},children:(0,n.jsx)(t9.$,{purpose:"section"})});if("PipelineSnapshot"!==_.pipelineSnapshotOrError.__typename)return(0,n.jsx)(i.x,{margin:20,children:(0,n.jsx)(s.t,{icon:"error",title:"Could not fetch job details"})});let{pipelineSnapshotOrError:I,instance:U}=_,D=(0,f.i)(I.solidHandles).map(e=>e.solid),M=(0,ri.V)(D,k),z=(0,rl.tc)({nodes:M.all,mode:ro.hS.FLAT}).boxes.map(e=>({x:e.x,name:e.node.name})),L=e=>u[e]===tM.VOA.FAILURE,V=j.filter(L),J=function(e){let t={[tM.VOA.SUCCESS]:0,[tM.VOA.NOT_STARTED]:0,[tM.VOA.FAILURE]:0,[tM.VOA.QUEUED]:0,[tM.VOA.STARTED]:0};for(let r of e)t[r.state]=(t[r.state]||0)+1;return t}(y.map(e=>({partitionKey:e,state:u[e]})));return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(tF.a7,{children:(0,n.jsxs)(i.x,{flex:{direction:"column",gap:24},children:[(0,n.jsxs)(rm,{title:(0,n.jsxs)(i.x,{flex:{justifyContent:"space-between"},children:[(0,n.jsx)("div",{children:"Partitions"}),x&&(0,n.jsxs)(i.x,{flex:{gap:4,alignItems:"center"},children:[(0,n.jsx)(t9.$,{purpose:"body-text"}),(0,n.jsx)(p.pN,{color:er.$(),children:"Refreshing..."})]})]}),children:[(0,n.jsx)(i.x,{children:"Select partitions to materialize. Click and drag to select a range on the timeline."}),(0,n.jsx)(re.x,{selected:y,setSelected:e=>{let t=new Set(e);b(j.filter(e=>t.has(e))),P.fromFailure&&e.filter(e=>!L(e)).length>0&&O({...P,fromFailure:!1})},health:{runStatusForPartitionKey:e=>u[e]},partitionKeys:j,dimensionType:(0,rs.vH)(j[0])?tM.iVC.TIME_WINDOW:tM.iVC.STATIC}),(0,n.jsx)(rn,{value:S,onChange:v,counts:J,allowed:P.fromFailure?[tM.VOA.FAILURE]:[tM.VOA.NOT_STARTED,tM.VOA.FAILURE,tM.VOA.QUEUED,tM.VOA.STARTED,tM.VOA.SUCCESS]})]}),V.length?(0,n.jsx)(rm,{title:"Reexecution",children:(0,n.jsx)(t5.X,{checked:P.fromFailure,disabled:!$.every(L),onChange:()=>{let e={...P,fromFailure:!P.fromFailure,reexecute:!P.reexecute};e.fromFailure&&v([tM.VOA.FAILURE]),A(""),O(e)},label:(0,n.jsxs)(i.x,{flex:{display:"inline-flex",alignItems:"center"},children:[(0,n.jsx)(i.x,{margin:{right:4},children:"Re-execute from failures"}),(0,n.jsx)(T.u,{placement:"top",content:"For each partition, if the most recent run failed, launch a re-execution starting from the steps that failed. Only applies for selections of failed partitions.",children:(0,n.jsx)(en.JO,{name:"info",color:er.md()})})]})})}):null,(0,n.jsx)(rm,{title:(0,n.jsxs)(i.x,{flex:{display:"inline-flex",alignItems:"center"},children:[(0,n.jsx)(i.x,{margin:{right:4},children:"Step subset"}),(0,n.jsx)(T.u,{placement:"top",content:"Applies a step-selection to each run for the requested partitions.",children:(0,n.jsx)(en.JO,{name:"info",color:er.md()})})]}),children:(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:12},children:[(0,n.jsx)(rp.j,{disabled:P.fromFailure,width:520,items:D,value:k,placeholder:"Type a step subset",onChange:A,autoApplyChanges:!0}),k?(0,n.jsxs)("div",{style:{color:er.$()},children:[z.length," step",1===z.length?"":"s"," selected"]}):null]})}),(0,n.jsxs)(rm,{title:"Tags",children:[(0,n.jsx)(rc.U,{tagsFromSession:C,onChange:E,open:N,onRequestClose:()=>w(!1)}),C.length?(0,n.jsx)("div",{style:{border:`1px solid ${er.im()}`,borderRadius:8,padding:3},children:(0,n.jsx)(rc.x,{tagsFromSession:C,onRequestEdit:()=>w(!0)})}):(0,n.jsx)("div",{children:(0,n.jsx)(K.zx,{onClick:()=>w(!0),children:"Add tags to backfill runs"})})]}),(0,n.jsxs)(i.x,{flex:{direction:"column",gap:16},children:[(0,t7.Rb)(U)?null:(0,n.jsx)(t7.So,{}),(0,n.jsx)(t7.d,{instance:U})]})]})}),(0,n.jsxs)(tF.cN,{children:[(0,n.jsx)(K.zx,{intent:"none",onClick:o,children:"Cancel"}),(0,n.jsx)(rh,{partitionNames:$,partitionSetName:t,reexecutionSteps:!P.fromFailure&&M.all.length<D.length?z.map(e=>e.name):void 0,fromFailure:P.fromFailure,tags:C,onSubmit:l,onSuccess:e=>{(0,t7.Xd)(g,e,!1),r?.(e,k)},onError:e=>{(0,t7.mv)(e)},repoAddress:c})]})]})},rh=e=>{let{partitionSetName:t,partitionNames:r,reexecutionSteps:i,fromFailure:s,tags:o,onSuccess:l,onError:d,onSubmit:c,repoAddress:p}=e,u=(0,ej.$)(p),x=a.useRef(!0),[m,{loading:j}]=(0,h.Db)(ra.f);a.useEffect(()=>(x.current=!0,()=>{x.current=!1}),[l]);let g=async()=>{c();let{data:e}=await m({variables:{backfillParams:{selector:{partitionSetName:t,repositorySelector:u},partitionNames:r,reexecutionSteps:i,fromFailure:s,tags:o}}});x.current&&(e&&"LaunchBackfillSuccess"===e.launchPartitionBackfill.__typename?l?.(e.launchPartitionBackfill.backfillId,!1):d?.(e))},f=r.length,y=i?.length,b=[j?`Submitting ${f} ${1===f?"run":"runs"}…`:f?`Submit ${f} ${1===f?"run":"runs"}`:"Select partitions to submit",!j&&y?`(${y} selected ${1===y?"step":"steps"})`:""].join(" ");return(0,n.jsx)(rd.C,{runCount:f,config:{title:b,icon:"open_in_new",disabled:!f||j,onClick:g}})},rx=(0,h.Ps)`
  query BackfillSelectorQuery($pipelineSelector: PipelineSelector!) {
    pipelineSnapshotOrError(activePipelineSelector: $pipelineSelector) {
      ... on PipelineSnapshot {
        id
        name
        solidHandles {
          handleID
          solid {
            name
            definition {
              name
            }
            inputs {
              dependsOn {
                solid {
                  name
                }
              }
            }
            outputs {
              dependedBy {
                solid {
                  name
                }
              }
            }
          }
          ...GraphExplorerSolidHandleFragment
        }
      }
    }
    instance {
      id
      ...UsingDefaultLauncherAlertInstanceFragment
      ...DaemonNotRunningAlertInstanceFragment
    }
  }

  ${y.Q}
  ${t7.op}
  ${t7.EC}
`,rm=e=>{let{title:t,children:r}=e;return(0,n.jsxs)(i.x,{flex:{direction:"column",gap:4},children:[(0,n.jsx)(p.pm,{children:t}),(0,n.jsx)(i.x,{flex:{direction:"column",gap:8},padding:{top:16},border:"top",children:r})]})};var rj=r(343),rg=r(97350);let rf=a.memo(e=>{let{partitionNames:t,jobDataByPartition:r,stepDataByPartition:o,title:l,yLabel:d,isJob:c,hiddenStepKeys:p}=e,[u,h]=(0,a.useState)(()=>({})),x=(0,a.useRef)(null),m=(0,rg.R)(),[j,g]=(0,a.useState)(t.length>1e3),f=j&&t.length>1e3,y=(0,a.useCallback)(e=>{let t=x.current;if(!t)return;let r=t.scales["x-axis-0"];if(!r)return;let{offsetX:n,offsetY:i}=e;if(!("click"===e.type&&n<=t.chartArea.right&&n>=t.chartArea.left&&i<=t.chartArea.bottom&&i>=t.chartArea.top)||!e.shiftKey)return;let s=r.getValueForPixel(n),o=t.data.labels[s];h(e=>({...e,[o]:!e[o]}))},[]),b=(0,a.useMemo)(()=>f?null:{title:l?{display:!0,text:l}:void 0,animation:!1,scales:d?{y:{id:"y",title:{display:!0,text:d,color:m[er.Aw()]},grid:{color:m[er.wL()]},ticks:{color:m[er.Aw()],font:{size:12,family:e5.b.monospace}}},x:{id:"x",title:{display:!0,text:l,color:m[er.Aw()]},grid:{color:m[er.wL()]},ticks:{color:m[er.Aw()],font:{size:12,family:e5.b.monospace}}}}:void 0,plugins:{legend:{display:!1,onClick:(e,t)=>{}}},onClick:y,maintainAspectRatio:!1},[y,m,f,l,d]),{jobData:S,stepData:v}=(0,a.useMemo)(()=>{if(f)return{jobData:[],stepData:{}};let e=[],n={};return t.forEach(t=>{let i=!!u[t];r&&e.push({x:t,y:i?void 0:r[t]}),o&&Object.entries(o[t]||{}).forEach(e=>{let[r,s]=e;!p?.includes(r)&&s&&(n[r]=[...n[r]||[],{x:t,y:i?void 0:s}])})}),Object.keys(n).forEach(e=>{n[e]=ry(t,n[e])}),{jobData:e,stepData:n}},[u,p,r,t,f,o]),$=c?"Total job":"Total pipeline",N=(0,a.useMemo)(()=>f?null:{labels:t,datasets:[...!r||p&&p.includes($)?[]:[{label:$,data:S,borderColor:m[er.im()],backgroundColor:m[er.M()]}],...Object.keys(v).map(e=>({label:e,data:v[e],borderColor:(0,eg.kU)(e),backgroundColor:m[er.M()]}))]},[$,p,S,r,t,m,f,v]);return N&&b?(0,n.jsx)(rb,{children:(0,n.jsx)(rj.x1,{data:()=>N,height:300,options:b,ref:x})}):(0,n.jsx)(s.t,{icon:"warning",title:"Large number of data points",description:(0,n.jsxs)(i.x,{flex:{direction:"column",gap:8},children:["There are ",e8.V.format(t.length)," datapoints in this graph. This might crash the browser.",(0,n.jsx)("div",{children:(0,n.jsx)(K.zx,{intent:"primary",onClick:()=>{g(!1)},children:"Show anyway"})})]})})}),ry=(e,t)=>{let r={};return t.forEach(e=>{r[e.x]=e.y}),e.map(e=>({x:e,y:r[e]}))},rb=es.ZP.div.withConfig({componentId:"sc-9b085522-0"})(["display:flex;color:",";padding:24px 12px;text-decoration:none;"],er.$());var rS=r(32996),rv=r(71291),r$=r(95590),rN=r(9875);let rw=new Map,rC=a.memo(e=>{let{partitionSetName:t,repoAddress:r}=e,o=(0,ej.$)(r),l=(0,a.useMemo)(()=>({partitionSetName:t,repositorySelector:o}),[t,o]),d=(0,a.useMemo)(()=>JSON.stringify(l),[l]),c=(0,a.useMemo)(()=>rw.get(d),[d]),p=(0,h.aM)(rP,{variables:{partitionSetName:t,repositorySelector:o},notifyOnNetworkStatusChange:!0,fetchPolicy:"no-cache"});(0,a.useLayoutEffect)(()=>{p&&rw.set(d,p)},[d,p]);let{data:u,loading:x}=p.data?p:c??p;if(!u)return x?(0,n.jsx)(i.x,{padding:32,flex:{direction:"column",alignItems:"center"},children:(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8,alignItems:"center"},children:[(0,n.jsx)(t9.$,{purpose:"body-text"}),(0,n.jsx)("div",{children:"Loading partitions…"})]})}):(0,n.jsx)(i.x,{padding:32,children:(0,n.jsx)(s.t,{icon:"error",title:"An error occurred",description:"An unexpected error occurred."})});let{partitionSetOrError:m}=u;if("PartitionSetNotFoundError"===m.__typename)return(0,n.jsx)(i.x,{padding:32,children:(0,n.jsx)(s.t,{icon:"search",title:"Partition set not found",description:m.message})});if("PythonError"===m.__typename)return(0,n.jsx)(i.x,{padding:32,children:(0,n.jsx)(eC.T9,{error:m})});if("PythonError"===m.partitionsOrError.__typename)return(0,n.jsx)(i.x,{padding:32,children:(0,n.jsx)(eC.T9,{error:m.partitionsOrError})});let j=m.partitionsOrError.results.map(e=>{let{name:t}=e;return t});return(0,n.jsx)(rk,{partitionNames:j,partitionSet:m,repoAddress:r,partitionsQueryResult:p})});function rE(e){return(0,a.useMemo)(()=>{let t={},r={};return e.forEach(e=>{if(!e.runsLoaded||0===e.runs.length)return;let n=e.runs.sort((e,t)=>e.startTime||0-(t.startTime||0)),i=n[n.length-1];t[e.name]={},r[e.name]=i?.endTime&&i?.startTime?i.endTime-i.startTime:void 0,i.stepStats.forEach(r=>{t[e.name][r.stepKey]=[r.endTime&&r.startTime?r.endTime-r.startTime:void 0]})}),{runDurationData:r,stepDurationData:t}},[e])}let rk=a.memo(e=>{let{partitionSet:t,partitionNames:r,repoAddress:s,partitionsQueryResult:o}=e,{permissions:{canLaunchPartitionBackfill:l},disabledReasons:d}=(0,tj.xz)(s.location),{viewport:c,containerProps:u}=(0,t2.S)(),[h,x]=(0,a.useState)(60),[m,j]=(0,a.useState)(0),[g,f]=(0,a.useState)(!1),[y,b]=(0,a.useState)(!1),[S,v]=(0,a.useState)(!1),$=(0,ej.$)(s),[N,w]=(0,a.useState)(0),C=(0,rN.T)({partitionSetName:t.name,partitionTagName:tL.H.Partition,partitionNames:r,pageSize:h,runsFilter:[],repositorySelector:$,jobName:t.pipelineName,offset:m,skipQuery:!g});(0,a.useEffect)(()=>{c.width&&!g&&x((0,rv.Rc)(c.width-r$.AQ))},[c.width,g,x]);let E=(0,a.useMemo)(()=>g?r.slice(Math.max(0,r.length-1-m-h),r.length-m):r,[m,h,r,g]),k=rE(C).stepDurationData,A=(0,a.useCallback)(()=>v(!0),[]),P=(0,a.useMemo)(()=>new Set(E),[E]),{partitionStatusesOrError:O}=t,R=(0,a.useMemo)(()=>"PartitionStatuses"===O.__typename?O.results:[],[O]),{runStatusData:_,runDurationData:F}=(0,a.useMemo)(()=>{let e={},t={};return R.forEach(r=>{e[r.partitionName]=r.runStatus||tM.VOA.NOT_STARTED,P.has(r.partitionName)&&(t[r.partitionName]=r.runDuration||void 0)}),{runStatusData:e,runDurationData:t}},[R,P]),I=(0,a.useMemo)(()=>({runStatusForPartitionKey:e=>_[e]}),[_]);return(0,n.jsxs)("div",{children:[(0,n.jsx)(tF.Vq,{canEscapeKeyClose:!S,canOutsideClickClose:!S,onClose:()=>b(!1),style:{width:800,zIndex:1e3},title:`Launch ${t.pipelineName} backfill`,isOpen:y,children:y&&(0,n.jsx)(ru,{partitionSetName:t.name,partitionNames:r,runStatusData:_,refreshing:o.loading,pipelineName:t.pipelineName,onCancel:()=>b(!1),onLaunch:(e,t)=>{w(N+1),b(!1)},onSubmit:A,repoAddress:s})}),(0,n.jsxs)(i.x,{flex:{justifyContent:"space-between",direction:"row",alignItems:"center"},border:"bottom",padding:{vertical:16,horizontal:24},children:[(0,n.jsx)(p.pm,{children:"Status"}),(0,n.jsxs)(i.x,{flex:{gap:8},children:[(0,n.jsx)(K.zx,{onClick:()=>f(!g),active:y,children:g?"Hide per-step status":"Show per-step status"}),(0,n.jsx)(K.zx,{onClick:()=>o.refetch(),loading:o.loading,disabled:o.loading,children:"Refresh"}),l?(0,n.jsx)(K.zx,{onClick:()=>{o.refetch(),b(!y)},icon:(0,n.jsx)(en.JO,{name:"add_circle"}),active:y,children:"Launch backfill…"}):(0,n.jsx)(T.u,{content:d.canLaunchPartitionBackfill,children:(0,n.jsx)(K.zx,{icon:(0,n.jsx)(en.JO,{name:"add_circle"}),disabled:!0,children:"Launch backfill…"})})]})]}),(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center"},border:"bottom",padding:{left:8},children:[(0,n.jsx)(rA,{count:r.length,label:"Total partitions"}),(0,n.jsx)(rA,{count:(0,a.useMemo)(()=>r.filter(e=>_[e]===tM.VOA.FAILURE).length,[r,_]),label:"Failed partitions"}),(0,n.jsx)(rA,{count:(0,a.useMemo)(()=>r.filter(e=>!_[e]||_[e]===tM.VOA.NOT_STARTED).length,[r,_]),label:"Missing partitions"})]}),(0,n.jsxs)(i.x,{padding:{vertical:16,horizontal:24},children:[(0,n.jsx)("div",{...u,children:(0,n.jsx)(rS.b,{partitionNames:r,health:I,selected:g?E:void 0,selectionWindowSize:h,onClick:(0,a.useCallback)(e=>{let t=r.length-1;j(Math.min(t,Math.max(0,t-r.indexOf(e)-.5*h))),g||f(!0)},[h,r,g]),tooltipMessage:"Click to view per-step status"})}),g?(0,n.jsx)(i.x,{margin:{top:16},children:(0,n.jsx)(rv.ux,{partitionNames:r,partitions:C,pipelineName:t.pipelineName,repoAddress:s,setPageSize:x,offset:m,setOffset:j})}):null]}),(0,n.jsx)(i.x,{padding:{horizontal:24,vertical:16},border:"top-and-bottom",children:(0,n.jsx)(p.pm,{children:"Run duration"})}),(0,n.jsx)(i.x,{margin:24,children:(0,n.jsx)(rf,{isJob:!0,title:"Execution time by partition",yLabel:"Execution time (secs)",partitionNames:g?E:r,jobDataByPartition:F})}),g?(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(i.x,{padding:{horizontal:24,vertical:16},children:(0,n.jsx)(p.pm,{children:"Step duration"})}),(0,n.jsx)(i.x,{margin:24,children:(0,n.jsx)(rf,{isJob:!0,title:"Execution time by partition",yLabel:"Execution time (secs)",partitionNames:E,stepDataByPartition:k})})]}):null,(0,n.jsx)(i.x,{padding:{horizontal:24,vertical:16},border:"top-and-bottom",style:{marginBottom:-1},children:(0,n.jsx)(p.pm,{children:"Backfill history"})}),(0,n.jsx)(i.x,{margin:{bottom:20},children:(0,n.jsx)(t3,{partitionSetName:t.name,repositorySelector:$,partitionNames:r,refetchCounter:N})})]})}),rA=e=>{let{count:t,label:r}=e;return(0,n.jsxs)(i.x,{padding:16,style:{flex:1},border:"right",children:[(0,n.jsx)("div",{style:{fontSize:18,marginBottom:4},children:(0,n.jsx)("strong",{children:t})}),(0,n.jsx)("div",{children:r})]})},rP=(0,h.Ps)`
  query PartitionsStatusQuery(
    $partitionSetName: String!
    $repositorySelector: RepositorySelector!
  ) {
    partitionSetOrError(
      repositorySelector: $repositorySelector
      partitionSetName: $partitionSetName
    ) {
      ... on PartitionSet {
        id
        ...OpJobPartitionSet
      }
      ... on PartitionSetNotFoundError {
        message
      }
      ...PythonErrorFragment
    }
  }

  fragment OpJobPartitionSet on PartitionSet {
    id
    name
    pipelineName
    partitionsOrError {
      ... on Partitions {
        results {
          name
        }
      }
      ...PythonErrorFragment
    }
    partitionStatusesOrError {
      ... on PartitionStatuses {
        results {
          id
          ...OpJobPartitionStatus
        }
      }
      ...PythonErrorFragment
    }
  }

  fragment OpJobPartitionStatus on PartitionStatus {
    id
    partitionName
    runStatus
    runDuration
  }

  ${x.B}
`;var rO=r(61915),rR=r(58779),r_=r(65470),rF=r(48764),rT=r(65152);let rI=e=>{let{partitionSetName:t,repoAddress:r,pipelineName:s}=e,{viewport:o,containerProps:l}=(0,t2.S)(),d=(0,ej.$)(r),c=(0,rR.uc)("*",{pipelineSelector:{pipelineName:s,repositoryName:r.name,repositoryLocationName:r.location}}),u=(0,a.useMemo)(()=>c.graphAssetKeys.filter(e=>c.assetGraphData?.nodes[L.Wu(e)]?.definition.isPartitioned),[c]),h=(0,rT.mR)(u.length?u:c.graphAssetKeys[0]?[c.graphAssetKeys[0]]:[]),{total:x,missing:m,merged:j}=(0,a.useMemo)(()=>{let e=(0,rs.$G)(h.filter(e=>e.dimensions.length>0)),t=e.dimensions.map(e=>({selectedKeys:e.partitionKeys,selectedRanges:[(0,rO.$0)(e)],dimension:e})),r=(0,rs.ju)(t,t=>e.stateForKeyIdx(t).includes(r_.yo.MISSING));return{merged:e,total:(0,rT.xD)(t),missing:r.length}},[h]),[g,f]=(0,a.useState)(60),[y,b]=(0,a.useState)(0),[S,v]=(0,a.useState)(!1);(0,a.useEffect)(()=>{o.width&&f((0,rv.Rc)(o.width-r$.AQ))},[o.width,f]);let $=j.dimensions.findIndex(rs.nv);-1===$&&($=0);let N=j.dimensions[$]?j.dimensions[$]:null,w=N?.partitionKeys||[],C=w.slice(Math.max(0,w.length-1-y-g),w.length-y);return(0,n.jsxs)("div",{children:[(0,n.jsxs)(i.x,{flex:{justifyContent:"space-between",direction:"row",alignItems:"center"},border:"bottom",padding:{vertical:16,horizontal:24},children:[(0,n.jsx)(p.pm,{children:"Status"}),(0,n.jsxs)(i.x,{flex:{gap:8},children:[(0,n.jsx)(K.zx,{onClick:()=>v(!S),children:S?"Hide per-asset status":"Show per-asset status"}),(0,n.jsx)(rF.XE,{scope:{all:c.graphQueryItems.map(e=>e.node),skipAllTerm:!0},preferredJobName:s})]})]}),(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center"},border:"bottom",padding:{left:8},children:[(0,n.jsx)(rA,{count:x,label:"Total partitions"}),(0,n.jsx)(rA,{count:m,label:"Missing partitions"})]}),(0,n.jsxs)(i.x,{padding:{vertical:16,horizontal:24},children:[(0,n.jsx)("div",{...l,children:(0,n.jsx)(rS.b,{partitionNames:w,splitPartitions:!!N&&!(0,rs.nv)(N),health:{ranges:j.rangesForSingleDimension($)},selected:C,selectionWindowSize:g,tooltipMessage:"Click to view per-asset status",onClick:e=>{let t=w.length-1;b(Math.min(t,Math.max(0,t-w.indexOf(e)-.5*g)))}})}),S&&N&&(0,n.jsx)(i.x,{margin:{top:16},children:(0,n.jsx)(rv.ac,{rangeDimensionIdx:$,rangeDimension:N,assetHealth:h,assetQueryItems:c.graphQueryItems,pipelineName:s,setPageSize:f,offset:y,setOffset:b})})]}),S&&(0,n.jsx)(rU,{repositorySelector:d,pipelineName:s,partitionSetName:t,multidimensional:(j?.dimensions.length||0)>1,dimensionName:N?N.name:null,dimensionKeys:w,selected:C,offset:y,pageSize:g}),(0,n.jsx)(i.x,{padding:{horizontal:24,vertical:16},border:"top-and-bottom",style:{marginBottom:-1},children:(0,n.jsx)(p.pm,{children:"Backfill history"})}),(0,n.jsx)(i.x,{margin:{bottom:20},children:(0,n.jsx)(t3,{partitionSetName:t,repositorySelector:d,partitionNames:w,refetchCounter:1})})]})},rU=e=>{let{repositorySelector:t,dimensionKeys:r,dimensionName:s,selected:o,pageSize:l,partitionSetName:a,multidimensional:d,pipelineName:c,offset:u}=e,{stepDurationData:h,runDurationData:x}=rE((0,rN.T)({partitionSetName:a,partitionTagName:d?`${tL.H.Partition}/${s}`:tL.H.Partition,partitionNames:r,repositorySelector:t,pageSize:l,runsFilter:[],jobName:c,offset:u,skipQuery:!s}));return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(i.x,{padding:{horizontal:24,vertical:16},border:"top-and-bottom",children:(0,n.jsx)(p.pm,{children:"Run duration"})}),(0,n.jsx)(i.x,{margin:24,children:(0,n.jsx)(rf,{isJob:!0,title:"Execution time by partition",yLabel:"Execution time (secs)",partitionNames:o,jobDataByPartition:x})}),(0,n.jsx)(i.x,{padding:{horizontal:24,vertical:16},border:"top-and-bottom",children:(0,n.jsx)(p.pm,{children:"Step durations"})}),(0,n.jsx)(i.x,{margin:24,children:(0,n.jsx)(rf,{isJob:!0,title:"Execution time by partition",yLabel:"Execution time (secs)",partitionNames:o,stepDataByPartition:h})})]})},rD=(0,h.Ps)`
  query AssetJobPartitionSetsQuery(
    $pipelineName: String!
    $repositoryName: String!
    $repositoryLocationName: String!
  ) {
    partitionSetsOrError(
      pipelineName: $pipelineName
      repositorySelector: {
        repositoryName: $repositoryName
        repositoryLocationName: $repositoryLocationName
      }
    ) {
      ... on PipelineNotFoundError {
        message
      }
      ... on PartitionSets {
        results {
          id
          name
          mode
          solidSelection
        }
      }
      ...PythonErrorFragment
    }
  }

  ${x.B}
`,rM=e=>{let{repoAddress:t}=e,r=(0,d.UO)(),{pipelinePath:o}=r,l=(0,b.p3)(o),{pipelineName:c}=l,p=(0,th.Ux)(t),u=p?.repository.pipelines.find(e=>e.name===c),x=!!u?.isJob,m=!!u?.isAssetJob;(0,tu.b)(l,x),(0,b.Bj)(r);let{partitionSet:j,partitionSetError:g}=function(e,t){let{data:r}=(0,h.aM)(rD,{skip:!t,variables:{repositoryLocationName:e.location,repositoryName:e.name,pipelineName:t}});return(0,a.useMemo)(()=>({partitionSet:r?.partitionSetsOrError.__typename==="PartitionSets"?r.partitionSetsOrError.results[0]:void 0,partitionSetError:r?.partitionSetsOrError.__typename==="PipelineNotFoundError"||r?.partitionSetsOrError.__typename==="PythonError"?r.partitionSetsOrError:void 0}),[r])}(t,c);return j||g?g?(0,n.jsx)(i.x,{padding:{vertical:64},children:(0,n.jsx)(s.t,{icon:"error",title:"Partitions",description:g.message})}):j?m?(0,n.jsx)(rI,{pipelineName:c,partitionSetName:j.name,repoAddress:t}):(0,n.jsx)(rC,{partitionSetName:j.name,repoAddress:t}):(0,n.jsx)(i.x,{padding:{vertical:64},children:(0,n.jsx)(s.t,{icon:"error",title:"Partitions",description:(0,n.jsxs)("div",{children:["There are no partition sets defined for ",x?"job":"pipeline"," ",(0,n.jsx)("code",{children:c}),"."]})})}):(0,n.jsx)(S.T,{purpose:"page"})},rz=e=>{let{repoAddress:t}=e;return(0,n.jsxs)("div",{style:{display:"flex",flexDirection:"column",minWidth:0,width:"100%",height:"100%"},children:[(0,n.jsx)(t1,{repoAddress:t}),(0,n.jsxs)(d.rs,{children:[(0,n.jsx)(A.A,{path:"/locations/:repoPath/pipeline_or_job/:pipelinePath(/?.*)",children:(0,n.jsx)(tf,{repoAddress:t})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/:pipelinePath/playground/setup","/locations/:repoPath/jobs/:pipelinePath/playground/setup"],children:(0,n.jsx)(tE,{repoAddress:t})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/:pipelinePath/playground/setup-from-run/:runId","/locations/:repoPath/jobs/:pipelinePath/playground/setup-from-run/:runId"],children:(0,n.jsx)(tN,{repoAddress:t})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/:pipelinePath/playground","/locations/:repoPath/jobs/:pipelinePath/playground"],children:(0,n.jsx)(tb.H,{repoAddress:t})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/:pipelinePath/runs/:runId","/locations/:repoPath/jobs/:pipelinePath/runs/:runId"],render:e=>(0,n.jsx)(d.l_,{to:`/runs/${e.match.params.runId}`})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/:pipelinePath/runs","/locations/:repoPath/jobs/:pipelinePath/runs"],children:(0,n.jsx)(ty.G,{repoAddress:t})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/:pipelinePath/partitions","/locations/:repoPath/jobs/:pipelinePath/partitions"],children:(0,n.jsx)(rM,{repoAddress:t})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/:pipelinePath/overview","/locations/:repoPath/jobs/:pipelinePath/overview"],render:e=>(0,n.jsx)(d.l_,{to:`/locations/${e.match.url.replace(/\/overview$/i,"")}`})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/(/?.*)","/locations/:repoPath/jobs/(/?.*)"],children:(0,n.jsx)(tm,{repoAddress:t})})]})]})};var rL=r(15686),rV=r(6750),rJ=r(34147),rH=r(581);let rB=["Succeeded","Failed","Missing","Pending"],rq=e=>{switch(e.runStatus){case null:return"Missing";case tM.VOA.SUCCESS:return"Succeeded";case tM.VOA.FAILURE:case tM.VOA.CANCELED:case tM.VOA.CANCELING:return"Failed";case tM.VOA.MANAGED:case tM.VOA.QUEUED:case tM.VOA.NOT_STARTED:case tM.VOA.STARTED:case tM.VOA.STARTING:return"Pending";default:return(0,eg.UT)(e.runStatus)}},rQ=(0,a.memo)(e=>{let{repoAddress:t,schedule:r}=e,i=(0,th.Ux)(t),{name:s,partitionSet:o,pipelineName:l}=r,d=o?.name,c=(0,th.E8)(i,l),u=(0,a.useMemo)(()=>{let e=d?eF().stringify({partitionSet:d},{addQueryPrefix:!0}):"";return`/${c?"jobs":"pipelines"}/${l}/partitions${e}`},[d,c,l]),x=(0,v.$U)(t,u),[m,{data:j,loading:g}]=(0,h.td)(rG,{variables:{scheduleSelector:{scheduleName:s,repositoryName:t.name,repositoryLocationName:t.location}}}),f=(0,a.useCallback)(()=>m(),[m]);return(0,n.jsxs)(rJ.Z,{direction:"column",spacing:4,children:[(0,n.jsx)(I.rU,{to:x,children:d}),(()=>{if(g)return(0,n.jsx)(p.YS,{style:{color:er.$()},children:"Loading…"});if(!j)return(0,n.jsx)(t_.Z,{onClick:f,children:(0,n.jsx)(p.YS,{children:"Show coverage"})});let e=j.scheduleOrError;return"Schedule"===e.__typename?(0,n.jsx)(rK,{schedule:e,partitionURL:x}):(0,n.jsx)(p.YS,{style:{color:er.Yn()},children:"Partition set not found!"})})()]})}),rK=e=>{let{schedule:t,partitionURL:r}=e,{partitionSet:i}=t;if(!i||"PartitionStatuses"!==i.partitionStatusesOrError.__typename)return(0,n.jsx)("span",{style:{color:er.$()},children:"None"});let s=i.partitionStatusesOrError.results,o={};return s.forEach(e=>{let t=rq(e);o[t]=[...o[t]||[],e]}),(0,n.jsx)(rH.Jy,{children:(0,n.jsx)("tbody",{children:rB.map(e=>e in o?(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{style:{width:"100px"},children:e}),(0,n.jsx)("td",{children:"Failed"===e||"Missing"===e?(0,n.jsx)(I.rU,{to:`${r}?showFailuresAndGapsOnly=true`,style:{color:er.Ep()},children:o[e].length}):o[e].length})]},e):null)})})},rG=(0,h.Ps)`
  query SchedulePartitionStatusQuery($scheduleSelector: ScheduleSelector!) {
    scheduleOrError(scheduleSelector: $scheduleSelector) {
      ... on Schedule {
        id
        ...SchedulePartitionStatusFragment
      }
    }
  }

  fragment SchedulePartitionStatusFragment on Schedule {
    id
    mode
    pipelineName
    partitionSet {
      id
      name
      partitionStatusesOrError {
        ... on PartitionStatuses {
          results {
            id
            ...SchedulePartitionStatusResult
          }
        }
      }
    }
  }

  fragment SchedulePartitionStatusResult on PartitionStatus {
    id
    partitionName
    runStatus
  }
`;var rW=r(99417);let rZ=e=>{let{repoAddress:t,schedule:r}=e,{permissions:{canStartSchedule:i,canStopRunningSchedule:s}}=(0,tj.xz)(t.location),{name:o}=r,l={...(0,ej.$)(t),scheduleName:o},[a,{loading:d}]=(0,h.Db)(rW.E1,{onCompleted:rW.JH}),c=i&&s,p=c?`In code, a default status for "${o}" has been set to "${r.defaultStatus}". Click here to reset the schedule status to track the status set in code.`:tj.Qr;return(0,n.jsx)(T.u,{content:(0,n.jsx)("div",{style:{maxWidth:"500px",wordBreak:"break-word"},children:p}),display:"flex",children:(0,n.jsx)(K.zx,{disabled:d||!c,onClick:()=>{a({variables:{scheduleSelector:l}})},children:"Reset schedule status"})})};var rY=r(89516),rX=r(24427),r0=r(55010),r1=r(60603),r2=r(49035),r4=r(76562),r8=r(8068),r3=r(68903),r6=r(24608),r9=r(57234),r5=r(58252);let r7=navigator.language,ne=e=>(0,n.jsx)(tF.Vq,{...e,style:{width:"70vw",display:"flex"},title:(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8,alignItems:"center"},children:[(0,n.jsx)(en.JO,{name:"preview_tick"}),(0,n.jsxs)("span",{children:["Preview tick result for ",e.name]})]}),children:(0,n.jsx)(nt,{...e})}),nt=e=>{let{repoAddress:t,name:r,onClose:o,jobName:l}=e,d=(0,m.Ke)(),[c,p]=(0,a.useState)(),u=(0,a.useMemo)(()=>({repositoryLocationName:t.location,repositoryName:t.name,scheduleName:r}),[t,r]),{data:x}=(0,h.aM)(nr,{variables:{scheduleSelector:u}}),[j,{loading:g}]=(0,h.Db)(ni),f=(0,r9.W)(),{timezone:[y]}=(0,a.useContext)(r3.JB),[b,S]=(0,a.useState)(!1),v=(0,a.useRef)(null),{viewport:$,containerProps:N}=(0,t2.S)(),[w,C]=(0,a.useState)(!1),[E,k]=(0,a.useState)(null),[A,P]=(0,a.useState)(null),O=(0,a.useMemo)(()=>x&&!g,[x,g]),R=(0,a.useCallback)(async()=>{if(!O)return;let e=(0,ej.$)(t),n=await j({variables:{selectorData:{...e,scheduleName:r},timestamp:v.current.ts}}),i=n.data?.scheduleDryRun;i?i?.__typename==="DryRunInstigationTick"?i.evaluationResult?.error?k(i.evaluationResult.error):P(i):i?.__typename==="ScheduleNotFoundError"?(0,r8._K)({title:"Schedule not found",body:`Could not find a schedule named: ${r}`}):k(i):(0,eg.UT)("scheduleDryRun Mutation returned no data??")},[O,j,t,r]),_=(0,a.useMemo)(()=>A&&u?(0,r5.G)(A,u):[],[u,A]),F=(0,a.useMemo)(()=>null!=_&&_.length>0,[_]),I=(0,a.useCallback)(async()=>{if(F){d("launch-all-schedule"),C(!0);try{_&&await f({executionParamsList:_},"toast",l)}catch(e){console.error(e)}C(!1),o()}},[F,_,l,f,o,d]),U=(0,a.useMemo)(()=>{if(w)return(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8,justifyContent:"center",alignItems:"center"},children:[(0,n.jsx)(t9.$,{purpose:"body-text"}),(0,n.jsx)("div",{children:"Launching runs"})]});if(!x)return(0,n.jsx)(i.x,{padding:{vertical:48},flex:{alignItems:"center",justifyContent:"center"},children:(0,n.jsx)(t9.$,{purpose:"page"})});if("PythonError"===x.scheduleOrError.__typename)return(0,n.jsx)(eC.T9,{error:x.scheduleOrError});if("ScheduleNotFoundError"===x.scheduleOrError.__typename)return(0,n.jsx)(s.t,{icon:"error",title:"Schedule not found",description:`Could not find a schedule named: ${r}`});if(A||E)return(0,n.jsx)(nn,{repoAddress:t,name:r,timestamp:v.current.ts,jobName:l,scheduleExecutionData:A,scheduleExecutionError:E});if(g)return(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8,justifyContent:"center",alignItems:"center"},children:[(0,n.jsx)(t9.$,{purpose:"body-text"}),(0,n.jsx)("div",{children:"Evaluating schedule"})]});{let e=x.scheduleOrError.potentialTickTimestamps.map(e=>({ts:e,label:(0,r6.j)({timestamp:{unix:e},locale:r7,timezone:y,timeFormat:{showTimezone:!0}})}));return v.current=c||e[0]||null,(0,n.jsxs)(i.x,{flex:{direction:"column",gap:8},children:[(0,n.jsx)(no,{children:"Select an evaluation time to simulate"}),(0,n.jsx)(r0.J,{isOpen:b,position:"bottom-left",fill:!0,content:(0,n.jsx)(r1.v2,{style:{maxHeight:"400px",overflow:"scroll",width:`${$.width}px`},children:e.map(e=>(0,n.jsx)(r1.sN,{text:(0,n.jsx)("div",{"data-testid":(0,rr.N)(`tick-${e.ts}`),children:e.label}),onClick:()=>{p(e),S(!1)}},e.ts))}),children:(0,n.jsx)("div",{...N,children:(0,n.jsx)(K.zx,{style:{flex:1,width:"100%"},rightIcon:(0,n.jsx)(en.JO,{name:"arrow_drop_down"}),onClick:()=>S(e=>!e),"data-testid":(0,rr.N)("tick-selection"),children:v.current?.label})})}),(0,n.jsxs)("div",{children:["Each evaluation of a schedule is called a tick, which is an opportunity for one or more runs to be launched. Ticks kick off runs, which either materialize a selection of assets or execute a ",(0,n.jsx)("a",{href:"https://docs.dagster.io/concepts/ops-jobs-graphs/jobs",children:"job"}),". You can preview the result for a given tick in the next step."]}),(0,n.jsxs)("div",{children:[(0,n.jsx)("a",{href:"https://docs.dagster.io/concepts/automation/schedules",children:"Learn more"})," about schedules"]})]})}},[w,x,A,E,g,t,r,l,c,b,$.width,N,y]),D=(0,a.useMemo)(()=>w?null:A||E?(0,n.jsx)(K.zx,{icon:(0,n.jsx)(en.JO,{name:"settings_backup_restore"}),"data-testid":(0,rr.N)("try-again"),onClick:()=>{P(null),k(null)},children:"Try again"}):null,[w,A,E]),M=(0,a.useMemo)(()=>{if(w)return(0,n.jsx)(i.x,{flex:{direction:"row",gap:8}});if(A||E){let e=A?.evaluationResult?.runRequests,t=e?.length||0,r=!E&&0===t;return E||r?(0,n.jsx)(i.x,{flex:{direction:"row",gap:8},children:(0,n.jsx)(K.zx,{onClick:o,children:"Close"})}):(0,n.jsxs)(i.x,{flex:{direction:"row",gap:8},children:[(0,n.jsx)(K.zx,{onClick:o,children:"Close"}),(0,n.jsx)(T.u,{canShow:!F||w,content:"Launches all runs and commits tick result",placement:"top-end",children:(0,n.jsx)(K.zx,{icon:(0,n.jsx)(en.JO,{name:"check_filled"}),intent:"primary",disabled:!F||w,onClick:I,"data-testid":(0,rr.N)("launch-all"),children:(0,n.jsx)("div",{children:"Launch all & commit tick result"})})})]})}return g?(0,n.jsx)(i.x,{flex:{direction:"row",gap:8},children:(0,n.jsx)(K.zx,{onClick:o,children:"Close"})}):(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(K.zx,{onClick:o,children:"Close"}),(0,n.jsx)(K.zx,{"data-testid":(0,rr.N)("continue"),intent:"primary",disabled:!O,onClick:()=>{R()},children:"Continue"})]})},[F,O,w,o,I,A,E,R,g]);return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(tF.a7,{children:(0,n.jsx)("div",{style:{minHeight:"300px"},children:U})}),(0,n.jsx)(tF.cN,{topBorder:!0,left:D,children:M})]})},nr=(0,h.Ps)`
  query getSchedule(
    $scheduleSelector: ScheduleSelector!
    $startTimestamp: Float
    $ticksAfter: Int
    $ticksBefore: Int
  ) {
    scheduleOrError(scheduleSelector: $scheduleSelector) {
      ... on PythonError {
        message
        stack
      }
      ... on Schedule {
        id
        name
        potentialTickTimestamps(
          startTimestamp: $startTimestamp
          upperLimit: $ticksAfter
          lowerLimit: $ticksBefore
        )
      }
    }
  }
`,nn=e=>{let{repoAddress:t,name:r,timestamp:o,jobName:l,scheduleExecutionData:d,scheduleExecutionError:c}=e,{timezone:[h]}=(0,a.useContext)(r3.JB),x=d?.evaluationResult,m=x?.runRequests?.length,j=c||x?.error;return(0,n.jsxs)(i.x,{flex:{direction:"column",gap:8},children:[(0,n.jsx)(i.x,{children:(0,n.jsxs)(ns,{children:[(0,n.jsxs)("div",{children:[(0,n.jsx)(p.pm,{children:"Result"}),(0,n.jsx)(i.x,{flex:{grow:1,alignItems:"center"},children:(0,n.jsx)("div",{children:j?(0,n.jsx)(u.V,{intent:"danger",children:"Failed"}):m?(0,n.jsxs)(u.V,{intent:"success",children:[m," run requests"]}):(0,n.jsx)(u.V,{intent:"warning",children:"Skipped"})})})]}),(0,n.jsxs)("div",{children:[(0,n.jsx)(p.pm,{children:"Tick"}),(0,n.jsx)(i.x,{flex:{grow:1,alignItems:"center"},children:(0,n.jsx)(p.fv,{children:(0,r6.j)({timestamp:{unix:o},locale:r7,timezone:h,timeFormat:{showTimezone:!0}})})})]})]})}),c?(0,n.jsx)(eC.T9,{error:c}):d&&x?x.error?(0,n.jsx)(eC.T9,{error:x.error}):x.runRequests?.length?(0,n.jsxs)(i.x,{flex:{direction:"column",gap:8},children:[(0,n.jsxs)(p.pm,{children:["Requested runs (",m,")"]}),(0,n.jsx)(r2.k,{runRequests:x.runRequests,repoAddress:t,isJob:!0,jobName:l,name:r})]}):(0,n.jsxs)(i.x,{flex:{direction:"column",gap:8},children:[(0,n.jsx)(p.pm,{style:{marginBottom:8},children:"Requested runs (0)"}),(0,n.jsx)("div",{children:(0,n.jsx)(nl,{children:(0,n.jsx)(s.t,{icon:"missing",title:"No runs requested",description:(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)("span",{children:"The schedule function was successfully evaluated but didn't return any run requests."}),(0,n.jsxs)("span",{children:[(0,n.jsx)("br",{}),"Skip reason:"," ",x?.skipReason?`"${x.skipReason}"`:"No skip reason was output"]})]})})})})]}):(0,n.jsx)(s.t,{title:"An unknown error occurred",description:(0,n.jsxs)("span",{children:["File an issue on"," ",(0,n.jsx)("a",{href:"https://github.com/dagster-io/dagster",target:"_blank",rel:"noreferrer",children:"Github"})," ","if you think this is a bug"]}),icon:"error"})]})},ni=(0,h.Ps)`
  mutation ScheduleDryRunMutation($selectorData: ScheduleSelector!, $timestamp: Float) {
    scheduleDryRun(selectorData: $selectorData, timestamp: $timestamp) {
      ...PythonErrorFragment
      ... on DryRunInstigationTick {
        timestamp
        evaluationResult {
          runRequests {
            ...RunRequestFragment
          }
          skipReason
          error {
            ...PythonErrorFragment
          }
        }
      }
      ... on ScheduleNotFoundError {
        scheduleName
      }
    }
  }
  ${x.B}
  ${r4.s}
`,ns=es.ZP.div.withConfig({componentId:"sc-4954c239-0"})(["display:grid;grid-template-columns:repeat(2,1fr);padding-bottom:12px;border-bottom:1px solid ",";margin-bottom:12px;","{padding-bottom:4px;display:block;}pre{margin:0;}button{margin-top:4px;}"],er.wL(),p.pm),no=es.ZP.div.withConfig({componentId:"sc-4954c239-1"})(["padding-bottom:2px;"]),nl=es.ZP.div.withConfig({componentId:"sc-4954c239-2"})(["","{margin:auto !important;width:unset !important;max-width:unset !important;}"],s.Y),na=e=>{let{name:t,repoAddress:r,jobName:s}=e,[o,l]=(0,a.useState)(!1);return(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:8},children:[(0,n.jsx)(K.zx,{icon:(0,n.jsx)(en.JO,{name:"preview_tick"}),onClick:()=>{l(!0)},children:"Preview tick result"}),(0,n.jsx)(ne,{isOpen:o,onClose:()=>{l(!1)},name:t,repoAddress:r,jobName:s})]})};var nd=r(58905);let nc={showSeconds:!0,showTimezone:!0},np=e=>{let{repoAddress:t,schedule:r,refreshState:s,assetSelection:o}=e,{cronSchedule:l,executionTimezone:a,futureTicks:d,name:h,partitionSet:x,pipelineName:m}=r,{scheduleState:j}=r,{status:f,ticks:y}=j,b=y.length>0?y[0]:null,S=f===tM.ynu.RUNNING;return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(c.m,{title:(0,n.jsxs)(p.X6,{style:{display:"flex",flexDirection:"row",gap:4},children:[(0,n.jsx)(I.rU,{to:"/automation",children:"Automation"}),(0,n.jsx)("span",{children:"/"}),h]}),tags:(0,n.jsxs)(u.V,{icon:"schedule",children:["Schedule in ",(0,n.jsx)(g.b,{repoAddress:t})]}),right:(0,n.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:8},children:[(0,n.jsx)(tD.xi,{refreshState:s}),(0,n.jsx)(na,{name:r.name,repoAddress:t,jobName:m})]})}),(0,n.jsx)(tU.fJ,{children:(0,n.jsxs)("tbody",{children:[r.description?(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Description"}),(0,n.jsx)("td",{children:r.description})]}):null,(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Latest tick"}),(0,n.jsx)("td",{children:b?(0,n.jsxs)(rJ.Z,{direction:"row",spacing:8,alignItems:"center",children:[(0,n.jsx)(tH.v,{timestamp:b.timestamp,timezone:a,timeFormat:nc}),(0,n.jsx)(nd.a,{tick:b,tickResultType:"runs"})]}):"Schedule has never run"})]}),d.results[0]&&S&&(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Next tick"}),(0,n.jsx)("td",{children:(0,n.jsx)(tH.v,{timestamp:d.results[0].timestamp,timezone:a,timeFormat:nc})})]}),r.pipelineName||o?(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Target"}),(0,n.jsx)(nu,{children:(0,n.jsx)(rX.p,{targets:r.pipelineName?[{pipelineName:r.pipelineName}]:null,repoAddress:t,assetSelection:o||null,automationType:"schedule"})})]}):null,(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:(0,n.jsx)(i.x,{flex:{alignItems:"center"},style:{height:"32px"},children:"Running"})}),(0,n.jsx)("td",{children:(0,n.jsxs)(i.x,{flex:{direction:"row",gap:12,alignItems:"center"},style:{height:"32px"},children:[(0,n.jsx)(tG.M,{repoAddress:t,schedule:r}),r.canReset&&(0,n.jsx)(rZ,{repoAddress:t,schedule:r})]})})]}),(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Partition set"}),(0,n.jsx)("td",{children:x?(0,n.jsx)(rQ,{schedule:r,repoAddress:t}):"None"})]}),(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Schedule"}),(0,n.jsx)("td",{children:l?(0,n.jsxs)(rJ.Z,{direction:"row",spacing:8,children:[(0,n.jsx)("span",{children:(0,rY.A)(l,{longTimezoneName:a||"UTC"})}),(0,n.jsxs)(p.EK,{children:["(",l,")"]})]}):(0,n.jsx)("div",{children:"—"})})]}),a?(0,n.jsxs)("tr",{children:[(0,n.jsx)("td",{children:"Execution timezone"}),(0,n.jsx)("td",{children:a})]}):null]})})]})},nu=es.ZP.td.withConfig({componentId:"sc-f08af491-0"})(["button{line-height:20px;}"]),nh=(0,h.Ps)`
  fragment ScheduleFragment on Schedule {
    id
    name
    cronSchedule
    executionTimezone
    pipelineName
    solidSelection
    mode
    description
    partitionSet {
      id
      name
    }
    defaultStatus
    canReset
    scheduleState {
      id
      ...InstigationStateFragment
    }
    futureTicks(limit: 5) {
      results {
        timestamp
      }
    }
  }

  ${rH.ai}
`;var nx=r(74050),nm=r(49438),nj=r(40782),ng=r(9790),nf=r(71902),ny=r(30916),nb=r(39554);let nS=e=>{(0,m.Px)();let{repoAddress:t}=e,{scheduleName:r}=(0,d.UO)(),{flagLegacyRunsPage:i}=(0,nm.gV)();(0,j.j)(`Schedule: ${r}`);let s={...(0,ej.$)(t),scheduleName:r},[o,l]=a.useState("ticks"),c=(0,h.aM)(n$,{variables:{scheduleSelector:s},notifyOnNetworkStatusChange:!0}),p=(0,h.aM)(rV.d,{variables:{scheduleSelector:s},notifyOnNetworkStatusChange:!0}),u=(0,tD.C4)(c,tD.dT),x=(0,tD.C4)(p,tD.dT),g=(0,tD.V5)(u,x),f=(0,n.jsxs)(O.mQ,{selectedTabId:o,onChange:l,children:[(0,n.jsx)(O.OK,{id:"ticks",title:"Tick history"}),(0,n.jsx)(O.OK,{id:"runs",title:"Run history"})]}),y=p.data?.scheduleOrError.__typename==="Schedule"?p.data.scheduleOrError.assetSelection:null,b=a.useMemo(()=>({tags:[{key:tL.H.ScheduleName,value:r},{key:tL.H.RepositoryLabelTag,value:(0,E.Wg)(t)}]}),[t,r]);return(0,n.jsx)(S.g,{queryResult:c,allowStaleData:!0,children:e=>{let{scheduleOrError:r,instance:s}=e;if("Schedule"!==r.__typename)return null;let l=!s.daemonHealth.daemonStatus.healthy;return(0,n.jsxs)(P.T,{children:[(0,n.jsx)(np,{repoAddress:t,schedule:r,refreshState:g,assetSelection:y}),l?(0,n.jsx)(nx.K,{daemonHealth:s.daemonHealth,padding:{vertical:16,horizontal:24}}):null,"ticks"===o?(0,n.jsx)(ng.H,{tabs:f,tickResultType:"runs",repoAddress:t,name:r.name}):i?(0,n.jsx)(nv,{repoAddress:t,schedule:r,tabs:f}):(0,n.jsx)(nb.g,{filter:b,actionBarComponents:f})]})}})},nv=e=>{let{repoAddress:t,schedule:r,highlightedIds:i,tabs:o}=e,l=(0,h.aM)(nN,{variables:{limit:20,filter:{tags:[{key:tL.H.ScheduleName,value:r.name},{key:tL.H.RepositoryLabelTag,value:(0,E.Wg)(t)}]}},notifyOnNetworkStatusChange:!0});(0,tD.C4)(l,tD.dT);let{data:a}=l;if(!a)return null;if("Runs"!==a.pipelineRunsOrError.__typename)return(0,n.jsx)(s.t,{icon:"error",title:"Query Error",description:a.pipelineRunsOrError.message});let d=a?.pipelineRunsOrError.results;return(0,n.jsx)(nf.A,{actionBarComponents:o,runs:d,highlightedIds:i,hideCreatedBy:!0})},n$=(0,h.Ps)`
  query ScheduleRootQuery($scheduleSelector: ScheduleSelector!) {
    scheduleOrError(scheduleSelector: $scheduleSelector) {
      ... on Schedule {
        id
        ...ScheduleFragment
      }
      ... on ScheduleNotFoundError {
        message
      }
      ...PythonErrorFragment
    }
    instance {
      id
      daemonHealth {
        id
        daemonStatus(daemonType: "SCHEDULER") {
          id
          healthy
        }
      }
      ...InstanceHealthFragment
    }
  }

  ${nh}
  ${x.B}
  ${nj.O}
`,nN=(0,h.Ps)`
  query PreviousRunsForScheduleQuery($filter: RunsFilter, $limit: Int) {
    pipelineRunsOrError(filter: $filter, limit: $limit) {
      ... on Runs {
        results {
          id
          ... on PipelineRun {
            ...RunTableRunFragment
          }
        }
      }
      ... on Error {
        message
      }
    }
  }

  ${ny.Z}
`;var nw=r(68856);let nC=()=>{let{repoPath:e}=(0,d.UO)(),t=(0,a.useContext)(C.C5),r=(0,k.g)(e),{loading:l}=t;if(!r)return(0,n.jsx)(i.x,{padding:{vertical:64},children:(0,n.jsx)(s.t,{icon:"error",title:"Invalid code location path",description:(0,n.jsxs)("div",{children:[(0,n.jsx)("div",{children:(0,n.jsx)("strong",{children:e})}),"  is not a valid code location path."]})})});let c=t.allRepos.find(e=>e.repository.name===r.name&&e.repositoryLocation.name===r.location);return!c&&l?(0,n.jsx)(i.x,{padding:{vertical:64},flex:{direction:"row",justifyContent:"center"},children:(0,n.jsx)(o.f,{label:`Loading ${(0,E.Uz)(r)}…`})}):(0,n.jsxs)(d.rs,{children:[(0,n.jsx)(A.A,{path:"/locations/:repoPath/graphs/(/?.*)",children:(0,n.jsx)($,{repoAddress:r})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/pipelines/(/?.*)","/locations/:repoPath/jobs/(/?.*)","/locations/:repoPath/pipeline_or_job/(/?.*)"],children:(0,n.jsx)(rz,{repoAddress:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/schedules/:scheduleName/:runTab?",children:(0,n.jsx)(nS,{repoAddress:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/sensors/:sensorName",children:(0,n.jsx)(nw.A,{repoAddress:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/resources/:resourceName",children:(0,n.jsx)(rL.r,{repoAddress:r})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/asset-groups/:groupName/list(/?.*)"],children:(0,n.jsx)(B,{repoAddress:r,tab:"list"})}),(0,n.jsx)(A.A,{path:["/locations/:repoPath/asset-groups/:groupName/(/?.*)","/locations/:repoPath/asset-groups/:groupName"],children:(0,n.jsx)(B,{repoAddress:r,tab:"lineage"})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath",exact:!0,children:(0,n.jsx)(td,{repoAddress:r})}),(0,n.jsx)(A.A,{path:"/locations/:repoPath/definitions",exact:!0,children:(0,n.jsx)(d.l_,{to:(0,v.$U)(r,"/assets")})}),c?(0,n.jsx)(A.A,{path:["/locations/:repoPath/assets","/locations/:repoPath/jobs","/locations/:repoPath/resources","/locations/:repoPath/schedules","/locations/:repoPath/sensors","/locations/:repoPath/graphs","/locations/:repoPath/ops/:name?"],exact:!0,children:(0,n.jsx)(e6,{repoAddress:r,repository:c.repository})}):null,(0,n.jsx)(A.A,{path:["/locations/:repoPath/*","/locations/:repoPath/"],children:(0,n.jsx)(d.l_,{to:(0,v.$U)(r,"/assets")})})]})},nE=()=>(0,n.jsx)(l.M,{children:(0,n.jsx)(d.rs,{children:(0,n.jsx)(A.A,{path:"/locations/:repoPath",children:(0,n.jsx)(nC,{})})})});var nk=nE}}]);
//# sourceMappingURL=1970.9ca6c4ccadf0a25d.js.map