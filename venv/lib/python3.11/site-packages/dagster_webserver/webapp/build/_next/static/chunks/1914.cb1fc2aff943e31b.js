"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[1914],{99478:function(e,n,r){r.d(n,{CY:function(){return y},N9:function(){return b}});var t=r(52322),s=r(2784),o=r(7267),l=r(67126),i=r(59920),a=r(24207),d=r(36381),p=r(20958),u=r(78491),h=r(69737),c=r(71469),x=r(44900),g=r(75442),m=r(20171),j=r(81535),f=r(39593),v=r(59923),$=r(92199);let y=()=>{(0,h.Px)();let e=(0,o.UO)(),n=(0,d.p3)(e["0"]),{pipelineName:r,snapshotId:s}=n,l=(0,o.k6)(),i=(0,j.Q)();return(0,m.j)(`Snapshot: ${r}${s?`@${s.slice(0,8)}`:""}`),(0,t.jsx)(b,{explorerPath:n,onChangeExplorerPath:(e,n)=>{l[n](`/snapshots/${(0,d.gY)(e)}`)},onNavigateToSourceAssetNode:(e,n)=>{let{assetKey:r}=n,t=(0,g.p)(r);e.metaKey?i(t):l.push((0,g.p)(r))}})},b=e=>{let{explorerPath:n,repoAddress:r,onChangeExplorerPath:o,onNavigateToSourceAssetNode:d,isGraph:u=!1}=e,[h,g]=(0,s.useState)({explodeComposites:n.explodeComposites??!1,preferAssetRendering:!0}),m=n.opNames.slice(0,n.opNames.length-1),j=(0,$._T)(r||null,n.pipelineName),f=(0,p.aM)(P,{variables:{snapshotPipelineSelector:n.snapshotId?void 0:j,snapshotId:n.snapshotId?n.snapshotId:void 0,rootHandleID:m.join("."),requestScopeHandleID:h.explodeComposites?void 0:m.join(".")}});return(0,t.jsx)(v.g,{queryResult:f,children:e=>{let{pipelineSnapshotOrError:s}=e;if("PipelineSnapshot"!==s.__typename)return(0,t.jsx)(a.Q,{isGraph:u,result:s,repoAddress:r});let p=s.solidHandle,m=h.explodeComposites?(0,l.i)(s.solidHandles):s.solidHandles,f=s.solidHandles.some(e=>e.solid.definition.assetNodes.length>0);return h.preferAssetRendering&&f?(0,t.jsx)(c.OK,{options:h,setOptions:g,fetchOptions:{pipelineSelector:j},explorerPath:n,onChangeExplorerPath:o,onNavigateToSourceAssetNode:d,viewType:x._4.JOB}):(0,t.jsx)(i.m0,{options:h,setOptions:g,explorerPath:n,onChangeExplorerPath:o,container:s,repoAddress:r,handles:m,parentHandle:p||void 0,isGraph:u,getInvocations:e=>m.filter(n=>n.solid.definition.name===e).map(e=>({handleID:e.handleID}))})}})},P=(0,p.Ps)`
  query PipelineExplorerRootQuery(
    $snapshotPipelineSelector: PipelineSelector
    $snapshotId: String
    $rootHandleID: String!
    $requestScopeHandleID: String
  ) {
    pipelineSnapshotOrError(
      snapshotId: $snapshotId
      activePipelineSelector: $snapshotPipelineSelector
    ) {
      ... on PipelineSnapshot {
        id
        name
        metadataEntries {
          ...MetadataEntryFragment
        }
        solidHandle(handleID: $rootHandleID) {
          ...GraphExplorerSolidHandleFragment
        }
        solidHandles(parentHandleID: $requestScopeHandleID) {
          handleID
          solid {
            name
            definition {
              assetNodes {
                id
                ...GraphExplorerAssetNodeFragment
              }
            }
          }
          ...GraphExplorerSolidHandleFragment
        }
        ...GraphExplorerFragment
      }
      ... on PipelineNotFoundError {
        message
      }
      ... on PipelineSnapshotNotFoundError {
        message
      }
      ...PythonErrorFragment
    }
  }

  ${f.i}
  ${i.Sm}
  ${i.Q}
  ${i.bg}
  ${u.B}
`},29664:function(e,n,r){r.d(n,{G:function(){return q}});var t=r(52322),s=r(49853),o=r(49308),l=r(14934),i=r(37483),a=r(43212),d=r(89891),p=r(80122),u=r(2784),h=r(7267),c=r(36381),x=r(36853),g=r(88610),m=r(92199),j=r(10539);let f=e=>{let{repoAddress:n,anyFilter:r,jobName:s,jobPath:o}=e,a=(0,m.Ux)(n),d=(0,m.Hb)(a,s);return(0,t.jsx)(l.x,{padding:{vertical:64},children:(0,t.jsx)(i.t,{icon:"run",title:"No runs found",description:n?d?(0,t.jsxs)(l.x,{flex:{direction:"column",gap:12},children:[(0,t.jsx)("div",{children:r?"There are no matching runs for these filters.":"You have not materialized any assets with this job yet."}),(0,t.jsx)("div",{children:(0,t.jsx)(g.A,{icon:(0,t.jsx)(x.JO,{name:"materialization"}),to:(0,j.$U)(n,`/jobs/${o}`),children:"Materialize an asset"})})]}):(0,t.jsxs)(l.x,{flex:{direction:"column",gap:12},children:[(0,t.jsx)("div",{children:r?"There are no matching runs for these filters.":"You have not launched any runs for this job yet."}),(0,t.jsx)("div",{children:(0,t.jsx)(g.A,{icon:(0,t.jsx)(x.JO,{name:"add_circle"}),to:(0,j.$U)(n,`/jobs/${o}/playground`),children:"Launch a run"})})]}):(0,t.jsx)("div",{children:"You have not launched any runs for this job."})})})};var v=r(76016),$=r(16336),y=r(69737),b=r(43984),P=r(33182),S=r(49767),E=r(42884),I=r(39554),C=r(52338),H=r(38160),R=r(22563);let _=["status","tag","id","created_date_before","created_date_after"],F=e=>{(0,y.Px)();let{pipelinePath:n}=(0,h.UO)(),{repoAddress:r=null}=e,o=(0,c.p3)(n),{pipelineName:i,snapshotId:p}=o,x=(0,m.Ux)(r),g=(0,m.E8)(x,i);(0,v.b)(o,g);let[j,F]=(0,C.oD)(_),k=(0,u.useMemo)(()=>[g?{token:"job",value:i}:{token:"pipeline",value:i},p?{token:"snapshotId",value:p}:null].filter(Boolean),[g,i,p]),N=(0,E.fn)(),A=(0,u.useMemo)(()=>{let e=[...j,...k];if(r){let n={token:"tag",value:`${b.H.RepositoryLabelTag}=${(0,R.Wg)(r)}`};e.push(n)}return{...(0,C.VH)(e),pipelineName:i,snapshotId:p}},[j,k,i,r,p]),O=(0,u.useCallback)(e=>{let n=(0,s.HY)(e);j.some(e=>(0,s.HY)(e)===n)||F([...j,e])},[j,F]),{entries:w,paginationProps:T,queryResult:D}=(0,H.a)(A,"all",N.value),Y=(0,$.C4)(D,$.dT),{button:B,activeFiltersJsx:q}=(0,C.Vv)({enabledFilters:_,tokens:j,onChange:F,loading:D.loading}),U=(0,t.jsxs)(l.x,{flex:{direction:"row",gap:8,alignItems:"center"},style:{width:"100%"},padding:{right:16},children:[B,N.element,(0,t.jsx)("div",{style:{flex:1}}),(0,t.jsx)($.xi,{refreshState:Y})]}),G=(0,t.jsxs)(l.x,{flex:{direction:"row",gap:4,alignItems:"center"},children:[k.map(e=>{let{token:n,value:r}=e;return(0,t.jsx)(a.V,{children:`${n}:${r}`},n)}),q,q.length>0&&(0,t.jsx)(d.Z,{onClick:()=>F([]),children:"Clear all"})]});return(0,t.jsx)(P.Lw.Provider,{value:{refetch:Y.refetch},children:D.error?(0,t.jsx)(S.f,{error:D.error}):(0,t.jsx)("div",{style:{minHeight:0},children:(0,t.jsx)(I.F,{entries:w,loading:D.loading,onAddTag:O,refetch:Y.refetch,actionBarComponents:U,belowActionBarComponents:G,paginationProps:T,filter:A,emptyState:()=>(0,t.jsx)(f,{repoAddress:r,anyFilter:j.length>0,jobName:i,jobPath:n})})})})};var k=r(20958),N=r(49438),A=r(78491),O=r(71902),w=r(30916),T=r(15332),D=r(59923),Y=r(85062);let B=["status","tag","id","created_date_before","created_date_after"],q=e=>{let{flagLegacyRunsPage:n}=(0,N.gV)();return n?(0,t.jsx)(U,{...e}):(0,t.jsx)(F,{...e})},U=e=>{(0,y.Px)();let{pipelinePath:n}=(0,h.UO)(),{repoAddress:r=null}=e,x=(0,c.p3)(n),{pipelineName:g,snapshotId:j}=x,S=(0,m.Ux)(r),E=(0,m.E8)(S,g);(0,v.b)(x,E);let[I,H]=(0,C.oD)(B),_=(0,u.useMemo)(()=>[E?{token:"job",value:g}:{token:"pipeline",value:g},j?{token:"snapshotId",value:j}:null].filter(Boolean),[E,g,j]),F=[...I,..._];if(r){let e={token:"tag",value:`${b.H.RepositoryLabelTag}=${(0,R.Wg)(r)}`};F.push(e)}let{queryResult:k,paginationProps:N}=(0,T.l)({query:G,pageSize:25,variables:{filter:{...(0,C.VH)(F),pipelineName:g,snapshotId:j}},nextCursorForResult:e=>{if("Runs"===e.pipelineRunsOrError.__typename)return e.pipelineRunsOrError.results[25-1]?.id},getResultArray:e=>e&&"Runs"===e.pipelineRunsOrError.__typename?e.pipelineRunsOrError.results:[]}),A=(0,u.useCallback)(e=>{let n=(0,s.HY)(e);I.some(e=>(0,s.HY)(e)===n)||H([...I,e])},[I,H]),w=(0,$.C4)(k,$.dT),{button:q,activeFiltersJsx:U}=(0,C.Vv)({enabledFilters:B,tokens:I,onChange:H,loading:k.loading});return(0,t.jsx)(P.Lw.Provider,{value:{refetch:k.refetch},children:(0,t.jsx)(o.T,{children:(0,t.jsx)(D.g,{queryResult:k,allowStaleData:!0,children:e=>{let{pipelineRunsOrError:s}=e;if("Runs"!==s.__typename)return(0,t.jsx)(l.x,{padding:{vertical:64},children:(0,t.jsx)(i.t,{icon:"error",title:"Query Error",description:s.message})});let o=s.results.slice(0,25),{hasNextCursor:u,hasPrevCursor:h}=N;return(0,t.jsxs)(t.Fragment,{children:[(0,t.jsx)(Y.Y,{$top:0,children:(0,t.jsx)(O.A,{runs:o,onAddTag:A,actionBarComponents:(0,t.jsxs)(l.x,{flex:{direction:"row",justifyContent:"space-between",grow:1,alignItems:"center",gap:4},margin:{right:8},children:[q,(0,t.jsx)($.xi,{refreshState:w})]}),belowActionBarComponents:(0,t.jsxs)(t.Fragment,{children:[_.map(e=>{let{token:n,value:r}=e;return(0,t.jsx)(a.V,{children:`${n}:${r}`},n)}),U.length?(0,t.jsxs)(t.Fragment,{children:[U,(0,t.jsx)(d.Z,{onClick:()=>{H([])},children:"Clear all"})]}):null]}),emptyState:()=>(0,t.jsx)(f,{repoAddress:r,anyFilter:I.length>0,jobName:g,jobPath:n})})}),u||h?(0,t.jsx)("div",{style:{marginTop:"20px"},children:(0,t.jsx)(p.pI,{...N})}):null]})}})})})},G=(0,k.Ps)`
  query PipelineRunsRootQuery($limit: Int, $cursor: String, $filter: RunsFilter!) {
    pipelineRunsOrError(limit: $limit, cursor: $cursor, filter: $filter) {
      ... on Runs {
        results {
          id
          ...RunTableRunFragment
        }
      }
      ... on InvalidPipelineRunsFilterError {
        message
      }
      ...PythonErrorFragment
    }
  }

  ${w.Z}
  ${A.B}
`}}]);
//# sourceMappingURL=1914.cb1fc2aff943e31b.js.map