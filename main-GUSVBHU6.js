var Q_=Object.defineProperty,X_=Object.defineProperties;var J_=Object.getOwnPropertyDescriptors;var _f=Object.getOwnPropertySymbols;var ey=Object.prototype.hasOwnProperty,ty=Object.prototype.propertyIsEnumerable;var yf=(e,n,t)=>n in e?Q_(e,n,{enumerable:!0,configurable:!0,writable:!0,value:t}):e[n]=t,V=(e,n)=>{for(var t in n||={})ey.call(n,t)&&yf(e,t,n[t]);if(_f)for(var t of _f(n))ty.call(n,t)&&yf(e,t,n[t]);return e},ye=(e,n)=>X_(e,J_(n));var $e=null,Io=!1,Fn=1,ny=null,Ie=Symbol("SIGNAL");function j(e){let n=$e;return $e=e,n}function So(){return $e}var Mi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Si(e){if(Io)throw new Error("");if($e===null)return;$e.consumerOnSignalRead(e);let n=$e.producersTail;if(n!==void 0&&n.producer===e)return;let t,i=$e.recomputing;if(i&&(t=n!==void 0?n.nextProducer:$e.producers,t!==void 0&&t.producer===e)){$e.producersTail=t,t.lastReadVersion=e.version,t.knownValidAtEpoch=Fn;return}let r=e.consumersTail;if(r!==void 0&&r.consumer===$e&&(!i||r.knownValidAtEpoch===Fn))return;let o=Ai($e),s={producer:e,consumer:$e,nextProducer:t,prevConsumer:void 0,knownValidAtEpoch:Fn,lastReadVersion:e.version,nextConsumer:void 0};$e.producersTail=s,n!==void 0?n.nextProducer=s:$e.producers=s,o&&Cf(e,s)}function bf(){Fn++}function lc(e){if(!(Ai(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===Fn)){if(!e.producerMustRecompute(e)&&!Ti(e)){cc(e);return}e.producerRecomputeValue(e),cc(e)}}function dc(e){if(e.consumers===void 0)return;let n=Io;Io=!0;try{for(let t=e.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||iy(i)}}finally{Io=n}}function uc(){return $e?.consumerAllowSignalWrites!==!1}function iy(e){e.dirty=!0,dc(e),e.consumerMarkedDirty?.(e)}function cc(e){e.dirty=!1,e.lastCleanEpoch=Fn}function Pn(e){return e&&wf(e),j(e)}function wf(e){if(e.producersTail?.knownValidAtEpoch===Fn){let n=e.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}e.producersTail=void 0,e.recomputing=!0}function ki(e,n){j(n),e&&xf(e)}function xf(e){e.recomputing=!1;let n=e.producersTail,t=n!==void 0?n.nextProducer:e.producers;if(t!==void 0){if(Ai(e))do t=fc(t);while(t!==void 0);n!==void 0?n.nextProducer=void 0:e.producers=void 0}}function Ti(e){for(let n=e.producers;n!==void 0;n=n.nextProducer){let t=n.producer,i=n.lastReadVersion;if(i!==t.version||(lc(t),i!==t.version))return!0}return!1}function un(e){if(Ai(e)){let n=e.producers;for(;n!==void 0;)n=fc(n)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function Cf(e,n){let t=e.consumersTail,i=Ai(e);if(t!==void 0?(n.nextConsumer=t.nextConsumer,t.nextConsumer=n):(n.nextConsumer=void 0,e.consumers=n),n.prevConsumer=t,e.consumersTail=n,!i)for(let r=e.producers;r!==void 0;r=r.nextProducer)Cf(r.producer,r)}function fc(e){let n=e.producer,t=e.nextProducer,i=e.nextConsumer,r=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ai(n)){let o=n.producers;for(;o!==void 0;)o=fc(o)}return t}function Ai(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function mc(e){ny?.(e)}function pc(e,n){return Object.is(e,n)}function wr(e,n){let t=Object.create(ry);t.computation=e,n!==void 0&&(t.equal=n);let i=()=>{if(lc(t),Si(t),t.value===Mo)throw t.error;return t.value};return i[Ie]=t,mc(t),i}var sc=Symbol("UNSET"),ac=Symbol("COMPUTING"),Mo=Symbol("ERRORED"),ry=ye(V({},Mi),{value:sc,dirty:!0,error:null,equal:pc,kind:"computed",producerMustRecompute(e){return e.value===sc||e.value===ac},producerRecomputeValue(e){if(e.value===ac)throw new Error("");let n=e.value;e.value=ac;let t=Pn(e),i,r=!1;try{i=e.computation(),j(null),r=n!==sc&&n!==Mo&&i!==Mo&&e.equal(n,i)}catch(o){i=Mo,e.error=o}finally{ki(e,t)}if(r){e.value=n;return}e.value=i,e.version++}});function oy(){throw new Error}var Df=oy;function Ef(e){Df(e)}function hc(e){Df=e}var sy=null;function gc(e,n){let t=Object.create(Cr);t.value=e,n!==void 0&&(t.equal=n);let i=()=>If(t);return i[Ie]=t,mc(t),[i,s=>xr(t,s),s=>Mf(t,s)]}function If(e){return Si(e),e.value}function xr(e,n){uc()||Ef(e),e.equal(e.value,n)||(e.value=n,ay(e))}function Mf(e,n){uc()||Ef(e),xr(e,n(e.value))}var Cr=ye(V({},Mi),{equal:pc,value:void 0,kind:"signal"});function ay(e){e.version++,bf(),dc(e),sy?.(e)}var vc=ye(V({},Mi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function _c(e){if(e.dirty=!1,e.version>0&&!Ti(e))return;e.version++;let n=Pn(e);try{e.cleanup(),e.fn()}finally{ki(e,n)}}var yc;function ko(){return yc}function It(e){let n=yc;return yc=e,n}var Sf=Symbol("NotFound");function Oi(e){return e===Sf||e?.name==="\u0275NotFound"}function H(e){return typeof e=="function"}function To(e){let t=e(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Ao=To(e=>function(t){e(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Ln(e,n){if(e){let t=e.indexOf(n);0<=t&&e.splice(t,1)}}var se=class e{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(H(i))try{i()}catch(o){n=o instanceof Ao?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{kf(o)}catch(s){n=n??[],s instanceof Ao?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Ao(n)}}add(n){var t;if(n&&n!==this)if(this.closed)kf(n);else{if(n instanceof e){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(n)}}_hasParent(n){let{_parentage:t}=this;return t===n||Array.isArray(t)&&t.includes(n)}_addParent(n){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(n),t):t?[t,n]:n}_removeParent(n){let{_parentage:t}=this;t===n?this._parentage=null:Array.isArray(t)&&Ln(t,n)}remove(n){let{_finalizers:t}=this;t&&Ln(t,n),n instanceof e&&n._removeParent(this)}};se.EMPTY=(()=>{let e=new se;return e.closed=!0,e})();var bc=se.EMPTY;function Oo(e){return e instanceof se||e&&"closed"in e&&H(e.remove)&&H(e.add)&&H(e.unsubscribe)}function kf(e){H(e)?e():e.unsubscribe()}var pt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Ri={setTimeout(e,n,...t){let{delegate:i}=Ri;return i?.setTimeout?i.setTimeout(e,n,...t):setTimeout(e,n,...t)},clearTimeout(e){let{delegate:n}=Ri;return(n?.clearTimeout||clearTimeout)(e)},delegate:void 0};function Ro(e){Ri.setTimeout(()=>{let{onUnhandledError:n}=pt;if(n)n(e);else throw e})}function Vn(){}var Tf=wc("C",void 0,void 0);function Af(e){return wc("E",void 0,e)}function Of(e){return wc("N",e,void 0)}function wc(e,n,t){return{kind:e,value:n,error:t}}var Bn=null;function Ni(e){if(pt.useDeprecatedSynchronousErrorHandling){let n=!Bn;if(n&&(Bn={errorThrown:!1,error:null}),e(),n){let{errorThrown:t,error:i}=Bn;if(Bn=null,t)throw i}}else e()}function Rf(e){pt.useDeprecatedSynchronousErrorHandling&&Bn&&(Bn.errorThrown=!0,Bn.error=e)}var jn=class extends se{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Oo(n)&&n.add(this)):this.destination=dy}static create(n,t,i){return new zt(n,t,i)}next(n){this.isStopped?Cc(Of(n),this):this._next(n)}error(n){this.isStopped?Cc(Af(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Cc(Tf,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},cy=Function.prototype.bind;function xc(e,n){return cy.call(e,n)}var Dc=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:t}=this;if(t.next)try{t.next(n)}catch(i){No(i)}}error(n){let{partialObserver:t}=this;if(t.error)try{t.error(n)}catch(i){No(i)}else No(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(t){No(t)}}},zt=class extends jn{constructor(n,t,i){super();let r;if(H(n)||!n)r={next:n??void 0,error:t??void 0,complete:i??void 0};else{let o;this&&pt.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&xc(n.next,o),error:n.error&&xc(n.error,o),complete:n.complete&&xc(n.complete,o)}):r=n}this.destination=new Dc(r)}};function No(e){pt.useDeprecatedSynchronousErrorHandling?Rf(e):Ro(e)}function ly(e){throw e}function Cc(e,n){let{onStoppedNotification:t}=pt;t&&Ri.setTimeout(()=>t(e,n))}var dy={closed:!0,next:Vn,error:ly,complete:Vn};var Fi=typeof Symbol=="function"&&Symbol.observable||"@@observable";function fn(e){return e}function Nf(e){return e.length===0?fn:e.length===1?e[0]:function(t){return e.reduce((i,r)=>r(i),t)}}var Q=(()=>{class e{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new e;return i.source=this,i.operator=t,i}subscribe(t,i,r){let o=fy(t)?t:new zt(t,i,r);return Ni(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=Ff(i),new i((r,o)=>{let s=new zt({next:a=>{try{t(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Fi](){return this}pipe(...t){return Nf(t)(this)}toPromise(t){return t=Ff(t),new t((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return e.create=n=>new e(n),e})();function Ff(e){var n;return(n=e??pt.Promise)!==null&&n!==void 0?n:Promise}function uy(e){return e&&H(e.next)&&H(e.error)&&H(e.complete)}function fy(e){return e&&e instanceof jn||uy(e)&&Oo(e)}function my(e){return H(e?.lift)}function K(e){return n=>{if(my(n))return n.lift(function(t){try{return e(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function te(e,n,t,i,r){return new Ec(e,n,t,i,r)}var Ec=class extends jn{constructor(n,t,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Pf=To(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var w=(()=>{class e extends Q{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Fo(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new Pf}next(t){Ni(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Ni(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Ni(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:o}=this;return i||r?bc:(this.currentObservers=null,o.push(t),new se(()=>{this.currentObservers=null,Ln(o,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:o}=this;i?t.error(r):o&&t.complete()}asObservable(){let t=new Q;return t.source=this,t}}return e.create=(n,t)=>new Fo(n,t),e})(),Fo=class extends w{constructor(n,t){super(),this.destination=n,this.source=t}next(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,n)}error(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,n)}complete(){var n,t;(t=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||t===void 0||t.call(n)}_subscribe(n){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(n))!==null&&i!==void 0?i:bc}};var zn=class extends w{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let t=super._subscribe(n);return!t.closed&&n.next(this._value),t}getValue(){let{hasError:n,thrownError:t,_value:i}=this;if(n)throw t;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Dr={now(){return(Dr.delegate||Date).now()},delegate:void 0};var Po=class extends w{constructor(n=1/0,t=1/0,i=Dr){super(),this._bufferSize=n,this._windowTime=t,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=t===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,t)}next(n){let{isStopped:t,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;t||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let t=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),t}_trimBuffer(){let{_bufferSize:n,_timestampProvider:t,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=t.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Lo=class extends se{constructor(n,t){super()}schedule(n,t=0){return this}};var Er={setInterval(e,n,...t){let{delegate:i}=Er;return i?.setInterval?i.setInterval(e,n,...t):setInterval(e,n,...t)},clearInterval(e){let{delegate:n}=Er;return(n?.clearInterval||clearInterval)(e)},delegate:void 0};var Vo=class extends Lo{constructor(n,t){super(n,t),this.scheduler=n,this.work=t,this.pending=!1}schedule(n,t=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,t)),this.pending=!0,this.delay=t,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,t),this}requestAsyncId(n,t,i=0){return Er.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,t,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return t;t!=null&&Er.clearInterval(t)}execute(n,t){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,t);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,t){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:t}=this,{actions:i}=t;this.work=this.state=this.scheduler=null,this.pending=!1,Ln(i,this),n!=null&&(this.id=this.recycleAsyncId(t,n,null)),this.delay=null,super.unsubscribe()}}};var Pi=class e{constructor(n,t=e.now){this.schedulerActionCtor=n,this.now=t}schedule(n,t=0,i){return new this.schedulerActionCtor(this,n).schedule(i,t)}};Pi.now=Dr.now;var Bo=class extends Pi{constructor(n,t=Pi.now){super(n,t),this.actions=[],this._active=!1}flush(n){let{actions:t}=this;if(this._active){t.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=t.shift());if(this._active=!1,i){for(;n=t.shift();)n.unsubscribe();throw i}}};var Hn=new Bo(Vo),Lf=Hn;var jo=new Q(e=>e.complete());function zo(e){return e&&H(e.schedule)}function Ic(e){return e[e.length-1]}function Vf(e){return H(Ic(e))?e.pop():void 0}function mn(e){return zo(Ic(e))?e.pop():void 0}function Bf(e,n){return typeof Ic(e)=="number"?e.pop():n}function zf(e,n,t,i){function r(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(h){s(h)}}function c(d){try{l(i.throw(d))}catch(h){s(h)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(e,n||[])).next())})}function jf(e){var n=typeof Symbol=="function"&&Symbol.iterator,t=n&&e[n],i=0;if(t)return t.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&i>=e.length&&(e=void 0),{value:e&&e[i++],done:!e}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Un(e){return this instanceof Un?(this.v=e,this):new Un(e)}function Hf(e,n,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(e,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(g){return function(_){return Promise.resolve(_).then(g,h)}}function a(g,_){i[g]&&(r[g]=function(b){return new Promise(function(I,G){o.push([g,b,I,G])>1||c(g,b)})},_&&(r[g]=_(r[g])))}function c(g,_){try{l(i[g](_))}catch(b){v(o[0][3],b)}}function l(g){g.value instanceof Un?Promise.resolve(g.value.v).then(d,h):v(o[0][2],g)}function d(g){c("next",g)}function h(g){c("throw",g)}function v(g,_){g(_),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Uf(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=e[Symbol.asyncIterator],t;return n?n.call(e):(e=typeof jf=="function"?jf(e):e[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(o){t[o]=e[o]&&function(s){return new Promise(function(a,c){s=e[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Ho=e=>e&&typeof e.length=="number"&&typeof e!="function";function Uo(e){return H(e?.then)}function $o(e){return H(e[Fi])}function Wo(e){return Symbol.asyncIterator&&H(e?.[Symbol.asyncIterator])}function Go(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function py(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var qo=py();function Zo(e){return H(e?.[qo])}function Yo(e){return Hf(this,arguments,function*(){let t=e.getReader();try{for(;;){let{value:i,done:r}=yield Un(t.read());if(r)return yield Un(void 0);yield yield Un(i)}}finally{t.releaseLock()}})}function Ko(e){return H(e?.getReader)}function fe(e){if(e instanceof Q)return e;if(e!=null){if($o(e))return hy(e);if(Ho(e))return gy(e);if(Uo(e))return vy(e);if(Wo(e))return $f(e);if(Zo(e))return _y(e);if(Ko(e))return yy(e)}throw Go(e)}function hy(e){return new Q(n=>{let t=e[Fi]();if(H(t.subscribe))return t.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function gy(e){return new Q(n=>{for(let t=0;t<e.length&&!n.closed;t++)n.next(e[t]);n.complete()})}function vy(e){return new Q(n=>{e.then(t=>{n.closed||(n.next(t),n.complete())},t=>n.error(t)).then(null,Ro)})}function _y(e){return new Q(n=>{for(let t of e)if(n.next(t),n.closed)return;n.complete()})}function $f(e){return new Q(n=>{by(e,n).catch(t=>n.error(t))})}function yy(e){return $f(Yo(e))}function by(e,n){var t,i,r,o;return zf(this,void 0,void 0,function*(){try{for(t=Uf(e);i=yield t.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=t.return)&&(yield o.call(t))}finally{if(r)throw r.error}}n.complete()})}function tt(e,n,t,i=0,r=!1){let o=n.schedule(function(){t(),r?e.add(this.schedule(null,i)):this.unsubscribe()},i);if(e.add(o),!r)return o}function Qo(e,n=0){return K((t,i)=>{t.subscribe(te(i,r=>tt(i,e,()=>i.next(r),n),()=>tt(i,e,()=>i.complete(),n),r=>tt(i,e,()=>i.error(r),n)))})}function Xo(e,n=0){return K((t,i)=>{i.add(e.schedule(()=>t.subscribe(i),n))})}function Wf(e,n){return fe(e).pipe(Xo(n),Qo(n))}function Gf(e,n){return fe(e).pipe(Xo(n),Qo(n))}function qf(e,n){return new Q(t=>{let i=0;return n.schedule(function(){i===e.length?t.complete():(t.next(e[i++]),t.closed||this.schedule())})})}function Zf(e,n){return new Q(t=>{let i;return tt(t,n,()=>{i=e[qo](),tt(t,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){t.error(s);return}o?t.complete():t.next(r)},0,!0)}),()=>H(i?.return)&&i.return()})}function Jo(e,n){if(!e)throw new Error("Iterable cannot be null");return new Q(t=>{tt(t,n,()=>{let i=e[Symbol.asyncIterator]();tt(t,n,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function Yf(e,n){return Jo(Yo(e),n)}function Kf(e,n){if(e!=null){if($o(e))return Wf(e,n);if(Ho(e))return qf(e,n);if(Uo(e))return Gf(e,n);if(Wo(e))return Jo(e,n);if(Zo(e))return Zf(e,n);if(Ko(e))return Yf(e,n)}throw Go(e)}function Li(e,n){return n?Kf(e,n):fe(e)}function Xe(...e){let n=mn(e);return Li(e,n)}function Mc(e,n){let t=H(e)?e:()=>e,i=r=>r.error(t());return new Q(n?r=>n.schedule(i,0,r):i)}function Qf(e){return e instanceof Date&&!isNaN(e)}function ae(e,n){return K((t,i)=>{let r=0;t.subscribe(te(i,o=>{i.next(e.call(n,o,r++))}))})}var{isArray:wy}=Array;function xy(e,n){return wy(n)?e(...n):e(n)}function Xf(e){return ae(n=>xy(e,n))}var{isArray:Cy}=Array,{getPrototypeOf:Dy,prototype:Ey,keys:Iy}=Object;function Jf(e){if(e.length===1){let n=e[0];if(Cy(n))return{args:n,keys:null};if(My(n)){let t=Iy(n);return{args:t.map(i=>n[i]),keys:t}}}return{args:e,keys:null}}function My(e){return e&&typeof e=="object"&&Dy(e)===Ey}function em(e,n){return e.reduce((t,i,r)=>(t[i]=n[r],t),{})}function tm(e,n,t,i,r,o,s,a){let c=[],l=0,d=0,h=!1,v=()=>{h&&!c.length&&!l&&n.complete()},g=b=>l<i?_(b):c.push(b),_=b=>{o&&n.next(b),l++;let I=!1;fe(t(b,d++)).subscribe(te(n,G=>{r?.(G),o?g(G):n.next(G)},()=>{I=!0},void 0,()=>{if(I)try{for(l--;c.length&&l<i;){let G=c.shift();s?tt(n,s,()=>_(G)):_(G)}v()}catch(G){n.error(G)}}))};return e.subscribe(te(n,g,()=>{h=!0,v()})),()=>{a?.()}}function pn(e,n,t=1/0){return H(n)?pn((i,r)=>ae((o,s)=>n(i,o,r,s))(fe(e(i,r))),t):(typeof n=="number"&&(t=n),K((i,r)=>tm(i,r,e,t)))}function es(e=1/0){return pn(fn,e)}function nm(){return es(1)}function Ir(...e){return nm()(Li(e,mn(e)))}function Sc(...e){let n=Vf(e),{args:t,keys:i}=Jf(e),r=new Q(o=>{let{length:s}=t;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let h=!1;fe(t[d]).subscribe(te(o,v=>{h||(h=!0,l--),a[d]=v},()=>c--,void 0,()=>{(!c||!h)&&(l||o.next(i?em(i,a):a),o.complete())}))}});return n?r.pipe(Xf(n)):r}function ts(e=0,n,t=Lf){let i=-1;return n!=null&&(zo(n)?t=n:i=n),new Q(r=>{let o=Qf(e)?+e-t.now():e;o<0&&(o=0);let s=0;return t.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Mt(...e){let n=mn(e),t=Bf(e,1/0),i=e;return i.length?i.length===1?fe(i[0]):es(t)(Li(i,n)):jo}function ve(e,n){return K((t,i)=>{let r=0;t.subscribe(te(i,o=>e.call(n,o,r++)&&i.next(o)))})}function im(e){return K((n,t)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,t.next(l)}s&&t.complete()},c=()=>{o=null,s&&t.complete()};n.subscribe(te(t,l=>{i=!0,r=l,o||fe(e(l)).subscribe(o=te(t,a,c))},()=>{s=!0,(!i||!o||o.closed)&&t.complete()}))})}function ns(e,n=Hn){return im(()=>ts(e,n))}function is(e){return K((n,t)=>{let i=null,r=!1,o;i=n.subscribe(te(t,void 0,void 0,s=>{o=fe(e(s,is(e)(n))),i?(i.unsubscribe(),i=null,o.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(t))})}function kc(e,n){return H(n)?pn(e,n,1):pn(e,1)}function $n(e,n=Hn){return K((t,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+e,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}t.subscribe(te(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,e),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Ht(e){return e<=0?()=>jo:K((n,t)=>{let i=0;n.subscribe(te(t,r=>{++i<=e&&(t.next(r),e<=i&&t.complete())}))})}function rm(){return K((e,n)=>{e.subscribe(te(n,Vn))})}function Vi(e){return ae(()=>e)}function Tc(e,n){return n?t=>Ir(n.pipe(Ht(1),rm()),t.pipe(Tc(e))):pn((t,i)=>fe(e(t,i)).pipe(Ht(1),Vi(t)))}function Ac(e,n=Hn){let t=ts(e,n);return Tc(()=>t)}function rs(e,n=fn){return e=e??Sy,K((t,i)=>{let r,o=!0;t.subscribe(te(i,s=>{let a=n(s);(o||!e(r,a))&&(o=!1,r=a,i.next(s))}))})}function Sy(e,n){return e===n}function Mr(e){return K((n,t)=>{try{n.subscribe(t)}finally{t.add(e)}})}function os(){return K((e,n)=>{let t,i=!1;e.subscribe(te(n,r=>{let o=t;t=r,i&&n.next([o,r]),i=!0}))})}function Sr(e={}){let{connector:n=()=>new w,resetOnError:t=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=e;return o=>{let s,a,c,l=0,d=!1,h=!1,v=()=>{a?.unsubscribe(),a=void 0},g=()=>{v(),s=c=void 0,d=h=!1},_=()=>{let b=s;g(),b?.unsubscribe()};return K((b,I)=>{l++,!h&&!d&&v();let G=c=c??n();I.add(()=>{l--,l===0&&!h&&!d&&(a=Oc(_,r))}),G.subscribe(I),!s&&l>0&&(s=new zt({next:qe=>G.next(qe),error:qe=>{h=!0,v(),a=Oc(g,t,qe),G.error(qe)},complete:()=>{d=!0,v(),a=Oc(g,i),G.complete()}}),fe(b).subscribe(s))})(o)}}function Oc(e,n,...t){if(n===!0){e();return}if(n===!1)return;let i=new zt({next:()=>{i.unsubscribe(),e()}});return fe(n(...t)).subscribe(i)}function ss(e,n,t){let i,r=!1;return e&&typeof e=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:t}=e:i=e??1/0,Sr({connector:()=>new Po(i,n,t),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Rc(e){return ve((n,t)=>e<=t)}function nt(...e){let n=mn(e);return K((t,i)=>{(n?Ir(e,t,n):Ir(e,t)).subscribe(i)})}function kr(e,n){return K((t,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();t.subscribe(te(i,c=>{r?.unsubscribe();let l=0,d=o++;fe(e(c,d)).subscribe(r=te(i,h=>i.next(n?n(c,h,d,l++):h),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function pe(e){return K((n,t)=>{fe(e).subscribe(te(t,()=>t.complete(),Vn)),!t.closed&&n.subscribe(t)})}function Wn(e,n,t){let i=H(e)||n||t?{next:e,error:n,complete:t}:e;return i?K((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(te(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):fn}var ms="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",D=class extends Error{code;constructor(n,t){super(zi(n,t)),this.code=n}};function ky(e){return`NG0${Math.abs(e)}`}function zi(e,n){return`${ky(e)}${n?": "+n:""}`}function de(e){for(let n in e)if(e[n]===de)return n;throw Error("")}function lm(e,n){for(let t in n)n.hasOwnProperty(t)&&!e.hasOwnProperty(t)&&(e[t]=n[t])}function ps(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(ps).join(", ")}]`;if(e==null)return""+e;let n=e.overriddenName||e.name;if(n)return`${n}`;let t=e.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function hs(e,n){return e?n?`${e} ${n}`:e:n||""}var Ty=de({__forward_ref__:de});function Wt(e){return e.__forward_ref__=Wt,e}function Ae(e){return Gc(e)?e():e}function Gc(e){return typeof e=="function"&&e.hasOwnProperty(Ty)&&e.__forward_ref__===Wt}function X(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function M(e){return{providers:e.providers||[],imports:e.imports||[]}}function gs(e){return Ay(e,vs)}function Ay(e,n){return e.hasOwnProperty(n)&&e[n]||null}function Oy(e){let n=e?.[vs]??null;return n||null}function Fc(e){return e&&e.hasOwnProperty(cs)?e[cs]:null}var vs=de({\u0275prov:de}),cs=de({\u0275inj:de}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,t){this._desc=n,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=X({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function qc(e){return e&&!!e.\u0275providers}var Zc=de({\u0275cmp:de}),Yc=de({\u0275dir:de}),Kc=de({\u0275pipe:de});var Ar=de({\u0275fac:de}),Qn=de({__NG_ELEMENT_ID__:de}),om=de({__NG_ENV_ID__:de});function gn(e){return Xc(e,"@Component"),e[Zc]||null}function Qc(e){return Xc(e,"@Directive"),e[Yc]||null}function dm(e){return Xc(e,"@Pipe"),e[Kc]||null}function Xc(e,n){if(e==null)throw new D(-919,!1)}function Jc(e){return typeof e=="string"?e:e==null?"":String(e)}var um=de({ngErrorCode:de}),Ry=de({ngErrorMessage:de}),Ny=de({ngTokenPath:de});function el(e,n){return fm("",-200,n)}function _s(e,n){throw new D(-201,!1)}function fm(e,n,t){let i=new D(n,e);return i[um]=n,i[Ry]=e,t&&(i[Ny]=t),i}function Fy(e){return e[um]}var Pc;function mm(){return Pc}function Je(e){let n=Pc;return Pc=e,n}function tl(e,n,t){let i=gs(e);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(n!==void 0)return n;_s(e,"")}var Hi=globalThis;var Py={},Gn=Py,Ly="__NG_DI_FLAG__",Lc=class{injector;constructor(n){this.injector=n}retrieve(n,t){let i=qn(t)||0;try{return this.injector.get(n,i&8?null:Gn,i)}catch(r){if(Oi(r))return r;throw r}}};function Vy(e,n=0){let t=ko();if(t===void 0)throw new D(-203,!1);if(t===null)return tl(e,void 0,n);{let i=By(n),r=t.retrieve(e,i);if(Oi(r)){if(i.optional)return null;throw r}return r}}function T(e,n=0){return(mm()||Vy)(Ae(e),n)}function m(e,n){return T(e,qn(n))}function qn(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function By(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function Vc(e){let n=[];for(let t=0;t<e.length;t++){let i=Ae(e[t]);if(Array.isArray(i)){if(i.length===0)throw new D(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=jy(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(T(r,o))}else n.push(T(i))}return n}function jy(e){return e[Ly]}function Zn(e,n){let t=e.hasOwnProperty(Ar);return t?e[Ar]:null}function pm(e,n,t){if(e.length!==n.length)return!1;for(let i=0;i<e.length;i++){let r=e[i],o=n[i];if(t&&(r=t(r),o=t(o)),o!==r)return!1}return!0}function hm(e){return e.flat(Number.POSITIVE_INFINITY)}function ys(e,n){e.forEach(t=>Array.isArray(t)?ys(t,n):n(t))}function nl(e,n,t){n>=e.length?e.push(t):e.splice(n,0,t)}function Fr(e,n){return n>=e.length-1?e.pop():e.splice(n,1)[0]}function gm(e,n){let t=[];for(let i=0;i<e;i++)t.push(n);return t}function vm(e,n,t,i){let r=e.length;if(r==n)e.push(t,i);else if(r===1)e.push(i,e[0]),e[0]=t;else{for(r--,e.push(e[r-1],e[r]);r>n;){let o=r-2;e[r]=e[o],r--}e[n]=t,e[n+1]=i}}function bs(e,n,t){let i=Ui(e,n);return i>=0?e[i|1]=t:(i=~i,vm(e,i,n,t)),i}function ws(e,n){let t=Ui(e,n);if(t>=0)return e[t|1]}function Ui(e,n){return zy(e,n,1)}function zy(e,n,t){let i=0,r=e.length>>t;for(;r!==i;){let o=i+(r-i>>1),s=e[o<<t];if(n===s)return o<<t;s>n?r=o:i=o+1}return~(r<<t)}var vn={},Ve=[],Xn=new y(""),Pr=new y("",-1),il=new y(""),ji=class{get(n,t=Gn){if(t===Gn){let r=fm("",-201);throw r.name="\u0275NotFound",r}return t}};function $i(e){return{\u0275providers:e}}function _m(e){return $i([{provide:Xn,multi:!0,useValue:e}])}function ym(...e){return{\u0275providers:rl(!0,e),\u0275fromNgModule:!0}}function rl(e,...n){let t=[],i=new Set,r,o=s=>{t.push(s)};return ys(n,s=>{let a=s;ls(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&bm(r,o),t}function bm(e,n){for(let t=0;t<e.length;t++){let{ngModule:i,providers:r}=e[t];ol(r,o=>{n(o,i)})}}function ls(e,n,t,i){if(e=Ae(e),!e)return!1;let r=null,o=Fc(e),s=!o&&gn(e);if(!o&&!s){let c=e.ngModule;if(o=Fc(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=e}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)ls(l,n,t,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;ys(o.imports,d=>{ls(d,n,t,i)&&(l||=[],l.push(d))}),l!==void 0&&bm(l,n)}if(!a){let l=Zn(r)||(()=>new r);n({provide:r,useFactory:l,deps:Ve},r),n({provide:il,useValue:r,multi:!0},r),n({provide:Xn,useValue:()=>T(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=e;ol(c,d=>{n(d,l)})}}else return!1;return r!==e&&e.providers!==void 0}function ol(e,n){for(let t of e)qc(t)&&(t=t.\u0275providers),Array.isArray(t)?ol(t,n):n(t)}var Hy=de({provide:String,useValue:de});function wm(e){return e!==null&&typeof e=="object"&&Hy in e}function Uy(e){return!!(e&&e.useExisting)}function $y(e){return!!(e&&e.useFactory)}function Yn(e){return typeof e=="function"}function xm(e){return!!e.useClass}var Lr=new y(""),as={},sm={},Nc;function Wi(){return Nc===void 0&&(Nc=new ji),Nc}var be=class{},Kn=class extends be{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,jc(n,s=>this.processProvider(s)),this.records.set(Pr,Bi(void 0,this)),r.has("environment")&&this.records.set(be,Bi(void 0,this));let o=this.records.get(Lr);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(il,Ve,{self:!0}))}retrieve(n,t){let i=qn(t)||0;try{return this.get(n,Gn,i)}catch(r){if(Oi(r))return r;throw r}}destroy(){Tr(this),this._destroyed=!0;let n=j(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),j(n)}}onDestroy(n){return Tr(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Tr(this);let t=It(this),i=Je(void 0),r;try{return n()}finally{It(t),Je(i)}}get(n,t=Gn,i){if(Tr(this),n.hasOwnProperty(om))return n[om](this);let r=qn(i),o,s=It(this),a=Je(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=Yy(n)&&gs(n);d&&this.injectableDefInScope(d)?l=Bi(Bc(n),as):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?Wi():this.parent;return t=r&8&&t===Gn?null:t,c.get(n,t)}catch(c){let l=Fy(c);throw l===-200||l===-201?new D(l,null):c}finally{Je(a),It(s)}}resolveInjectorInitializers(){let n=j(null),t=It(this),i=Je(void 0),r;try{let o=this.get(Xn,Ve,{self:!0});for(let s of o)s()}finally{It(t),Je(i),j(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Ae(n);let t=Yn(n)?n:Ae(n&&n.provide),i=Gy(n);if(!Yn(n)&&n.multi===!0){let r=this.records.get(t);r||(r=Bi(void 0,as,!0),r.factory=()=>Vc(r.multi),this.records.set(t,r)),t=n,r.multi.push(n)}this.records.set(t,i)}hydrate(n,t,i){let r=j(null);try{if(t.value===sm)throw el("");return t.value===as&&(t.value=sm,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&Zy(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{j(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let t=Ae(n.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(n){let t=this._onDestroyHooks.indexOf(n);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Bc(e){let n=gs(e),t=n!==null?n.factory:Zn(e);if(t!==null)return t;if(e instanceof y)throw new D(-204,!1);if(e instanceof Function)return Wy(e);throw new D(-204,!1)}function Wy(e){if(e.length>0)throw new D(-204,!1);let t=Oy(e);return t!==null?()=>t.factory(e):()=>new e}function Gy(e){if(wm(e))return Bi(void 0,e.useValue);{let n=sl(e);return Bi(n,as)}}function sl(e,n,t){let i;if(Yn(e)){let r=Ae(e);return Zn(r)||Bc(r)}else if(wm(e))i=()=>Ae(e.useValue);else if($y(e))i=()=>e.useFactory(...Vc(e.deps||[]));else if(Uy(e))i=(r,o)=>T(Ae(e.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Ae(e&&(e.useClass||e.provide));if(qy(e))i=()=>new r(...Vc(e.deps));else return Zn(r)||Bc(r)}return i}function Tr(e){if(e.destroyed)throw new D(-205,!1)}function Bi(e,n,t=!1){return{factory:e,value:n,multi:t?[]:void 0}}function qy(e){return!!e.deps}function Zy(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function Yy(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function jc(e,n){for(let t of e)Array.isArray(t)?jc(t,n):t&&qc(t)?jc(t.\u0275providers,n):n(t)}function Gi(e,n){let t;e instanceof Kn?(Tr(e),t=e):t=new Lc(e);let i,r=It(t),o=Je(void 0);try{return n()}finally{It(r),Je(o)}}function Cm(){return mm()!==void 0||ko()!=null}var ht=0,E=1,O=2,Me=3,it=4,Be=5,qi=6,Zi=7,Oe=8,Gt=9,kt=10,ge=11,Yi=12,al=13,_n=14,Ye=15,yn=16,Jn=17,Tt=18,qt=19,cl=20,Ut=21,xs=22,hn=23,et=24,ei=25,bn=26,xe=27,Dm=1;var wn=7,Vr=8,ti=9,Re=10;function Zt(e){return Array.isArray(e)&&typeof e[Dm]=="object"}function rt(e){return Array.isArray(e)&&e[Dm]===!0}function ll(e){return(e.flags&4)!==0}function Yt(e){return e.componentOffset>-1}function Br(e){return(e.flags&1)===1}function At(e){return!!e.template}function Ki(e){return(e[O]&512)!==0}function ni(e){return(e[O]&256)===256}var dl="svg",Em="math";function ot(e){for(;Array.isArray(e);)e=e[ht];return e}function ul(e,n){return ot(n[e])}function gt(e,n){return ot(n[e.index])}function Cs(e,n){return e.data[n]}function Im(e,n){return e[n]}function st(e,n){let t=n[e];return Zt(t)?t:t[ht]}function Mm(e){return(e[O]&4)===4}function Ds(e){return(e[O]&128)===128}function Sm(e){return rt(e[Me])}function Ot(e,n){return n==null?null:e[n]}function fl(e){e[Jn]=0}function ml(e){e[O]&1024||(e[O]|=1024,Ds(e)&&ii(e))}function km(e,n){for(;e>0;)n=n[_n],e--;return n}function jr(e){return!!(e[O]&9216||e[et]?.dirty)}function Es(e){e[kt].changeDetectionScheduler?.notify(8),e[O]&64&&(e[O]|=1024),jr(e)&&ii(e)}function ii(e){e[kt].changeDetectionScheduler?.notify(0);let n=$t(e);for(;n!==null&&!(n[O]&8192||(n[O]|=8192,!Ds(n)));)n=$t(n)}function Is(e,n){if(ni(e))throw new D(911,!1);e[Ut]===null&&(e[Ut]=[]),e[Ut].push(n)}function Tm(e,n){if(e[Ut]===null)return;let t=e[Ut].indexOf(n);t!==-1&&e[Ut].splice(t,1)}function $t(e){let n=e[Me];return rt(n)?n[Me]:n}function pl(e){return e[Zi]??=[]}function hl(e){return e.cleanup??=[]}function Am(e,n,t,i){let r=pl(n);r.push(t),e.firstCreatePass&&hl(e).push(i,r.length-1)}var U={lFrame:Hm(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var zc=!1;function Om(){return U.lFrame.elementDepthCount}function Rm(){U.lFrame.elementDepthCount++}function gl(){U.lFrame.elementDepthCount--}function vl(){return U.bindingsEnabled}function _l(){return U.skipHydrationRootTNode!==null}function yl(e){return U.skipHydrationRootTNode===e}function bl(){U.skipHydrationRootTNode=null}function q(){return U.lFrame.lView}function De(){return U.lFrame.tView}function Rt(e){return U.lFrame.contextLView=e,e[Oe]}function Nt(e){return U.lFrame.contextLView=null,e}function Ne(){let e=wl();for(;e!==null&&e.type===64;)e=e.parent;return e}function wl(){return U.lFrame.currentTNode}function Nm(){let e=U.lFrame,n=e.currentTNode;return e.isParent?n:n.parent}function Qi(e,n){let t=U.lFrame;t.currentTNode=e,t.isParent=n}function xl(){return U.lFrame.isParent}function Cl(){U.lFrame.isParent=!1}function Fm(){return U.lFrame.contextLView}function Dl(){return zc}function Or(e){let n=zc;return zc=e,n}function Pm(e){return U.lFrame.bindingIndex=e}function Xi(){return U.lFrame.bindingIndex++}function El(e){let n=U.lFrame,t=n.bindingIndex;return n.bindingIndex=n.bindingIndex+e,t}function Lm(){return U.lFrame.inI18n}function Vm(e,n){let t=U.lFrame;t.bindingIndex=t.bindingRootIndex=e,Ms(n)}function Bm(){return U.lFrame.currentDirectiveIndex}function Ms(e){U.lFrame.currentDirectiveIndex=e}function jm(e){let n=U.lFrame.currentDirectiveIndex;return n===-1?null:e[n]}function Ss(){return U.lFrame.currentQueryIndex}function zr(e){U.lFrame.currentQueryIndex=e}function Ky(e){let n=e[E];return n.type===2?n.declTNode:n.type===1?e[Be]:null}function Il(e,n,t){if(t&4){let r=n,o=e;for(;r=r.parent,r===null&&!(t&1);)if(r=Ky(o),r===null||(o=o[_n],r.type&10))break;if(r===null)return!1;n=r,e=o}let i=U.lFrame=zm();return i.currentTNode=n,i.lView=e,!0}function ks(e){let n=zm(),t=e[E];U.lFrame=n,n.currentTNode=t.firstChild,n.lView=e,n.tView=t,n.contextLView=e,n.bindingIndex=t.bindingStartIndex,n.inI18n=!1}function zm(){let e=U.lFrame,n=e===null?null:e.child;return n===null?Hm(e):n}function Hm(e){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=n),n}function Um(){let e=U.lFrame;return U.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var Ml=Um;function Ts(){let e=Um();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function $m(e){return(U.lFrame.contextLView=km(e,U.lFrame.contextLView))[Oe]}function xn(){return U.lFrame.selectedIndex}function Cn(e){U.lFrame.selectedIndex=e}function As(){let e=U.lFrame;return Cs(e.tView,e.selectedIndex)}function vt(){U.lFrame.currentNamespace=dl}function Dn(){Qy()}function Qy(){U.lFrame.currentNamespace=null}function Sl(){return U.lFrame.currentNamespace}var Wm=!0;function Os(){return Wm}function Rs(e){Wm=e}function Hc(e,n=null,t=null,i){let r=Gm(e,n,t,i);return r.resolveInjectorInitializers(),r}function Gm(e,n=null,t=null,i,r=new Set){let o=[t||Ve,ym(e)],s;return new Kn(o,n||Wi(),s||null,r)}var ee=class e{static THROW_IF_NOT_FOUND=Gn;static NULL=new ji;static create(n,t){if(Array.isArray(n))return Hc({name:""},t,n,"");{let i=n.name??"";return Hc({name:i},n.parent,n.providers,i)}}static \u0275prov=X({token:e,providedIn:"any",factory:()=>T(Pr)});static __NG_ELEMENT_ID__=-1},P=new y(""),at=(()=>{class e{static __NG_ELEMENT_ID__=Xy;static __NG_ENV_ID__=t=>t}return e})(),ds=class extends at{_lView;constructor(n){super(),this._lView=n}get destroyed(){return ni(this._lView)}onDestroy(n){let t=this._lView;return Is(t,n),()=>Tm(t,n)}};function Xy(){return new ds(q())}var qm=!1,Zm=new y(""),ri=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new zn(!1);debugTaskTracker=m(Zm,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Q(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=X({token:e,providedIn:"root",factory:()=>new e})}return e})(),Uc=class extends w{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Cm()&&(this.destroyRef=m(at,{optional:!0})??void 0,this.pendingTasks=m(ri,{optional:!0})??void 0)}emit(n){let t=j(null);try{super.next(n)}finally{j(t)}}subscribe(n,t,i){let r=n,o=t||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof se&&n.add(a),a}wrapInTimeout(n){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},ce=Uc;function us(...e){}function kl(e){let n,t;function i(){e=us;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{e(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{e(),i()})),()=>i()}function Ym(e){return queueMicrotask(()=>e()),()=>{e=us}}var Tl="isAngularZone",Rr=Tl+"_ID",Jy=0,C=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new ce(!1);onMicrotaskEmpty=new ce(!1);onStable=new ce(!1);onError=new ce(!1);constructor(n){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=qm}=n;if(typeof Zone>"u")throw new D(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,nb(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Tl)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new D(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new D(909,!1)}run(n,t,i){return this._inner.run(n,t,i)}runTask(n,t,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,eb,us,us);try{return o.runTask(s,t,i)}finally{o.cancelTask(s)}}runGuarded(n,t,i){return this._inner.runGuarded(n,t,i)}runOutsideAngular(n){return this._outer.run(n)}},eb={};function Al(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function tb(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function n(){kl(()=>{e.callbackScheduled=!1,$c(e),e.isCheckStableRunning=!0,Al(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{n()}):e._outer.run(()=>{n()}),$c(e)}function nb(e){let n=()=>{tb(e)},t=Jy++;e._inner=e._inner.fork({name:"angular",properties:{[Tl]:!0,[Rr]:t,[Rr+t]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(ib(c))return i.invokeTask(o,s,a,c);try{return am(e),i.invokeTask(o,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&n(),cm(e)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return am(e),i.invoke(o,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!rb(c)&&n(),cm(e)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,$c(e),Al(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function $c(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function am(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function cm(e){e._nesting--,Al(e)}var Nr=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new ce;onMicrotaskEmpty=new ce;onStable=new ce;onError=new ce;run(n,t,i){return n.apply(t,i)}runGuarded(n,t,i){return n.apply(t,i)}runOutsideAngular(n){return n()}runTask(n,t,i,r){return n.apply(t,i)}};function ib(e){return Km(e,"__ignore_ng_zone__")}function rb(e){return Km(e,"__scheduler_tick__")}function Km(e,n){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[n]===!0}var Ze=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Kt=new y("",{factory:()=>{let e=m(C),n=m(be),t;return i=>{e.runOutsideAngular(()=>{n.destroyed&&!t?setTimeout(()=>{throw i}):(t??=n.get(Ze),t.handleError(i))})}}}),Qm={provide:Xn,useValue:()=>{let e=m(Ze,{optional:!0})},multi:!0},ob=new y("",{factory:()=>{let e=m(P).defaultView;if(!e)return;let n=m(Kt),t=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{e.addEventListener("unhandledrejection",t),e.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),m(at).onDestroy(()=>{e.removeEventListener("error",i),e.removeEventListener("unhandledrejection",t)})}});function Ol(){return $i([_m(()=>{m(ob)})])}function Ee(e,n){let[t,i,r]=gc(e,n?.equal),o=t,s=o[Ie];return o.set=i,o.update=r,o.asReadonly=Xm.bind(o),o}function Xm(){let e=this[Ie];if(e.readonlyFn===void 0){let n=()=>this();n[Ie]=e,e.readonlyFn=n}return e.readonlyFn}var oi=new y("",{factory:()=>sb}),sb="ng";var Ns=new y(""),si=new y("",{providedIn:"platform",factory:()=>"unknown"}),Hr=new y(""),ai=new y("",{factory:()=>m(P).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Ji=(()=>{class e{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=ab}return e})();function ab(){return new Ji(q(),Ne())}var St=class{},Ur=new y("",{factory:()=>!0});var Rl=new y(""),Fs=(()=>{class e{static \u0275prov=X({token:e,providedIn:"root",factory:()=>new Wc})}return e})(),Wc=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let t=n.zone,i=this.queues.get(t);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let t=n.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[t,i]of this.queues)t===null?n||=this.flushQueue(i):n||=t.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let t=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},fs=class{[Ie];constructor(n){this[Ie]=n}destroy(){this[Ie].destroy()}};function ci(e,n){let t=n?.injector??m(ee),i=n?.manualCleanup!==!0?t.get(at):null,r,o=t.get(Ji,null,{optional:!0}),s=t.get(St);return o!==null?(r=db(o.view,s,e),i instanceof ds&&i._lView===o.view&&(i=null)):r=ub(e,t.get(Fs),s),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new fs(r)}var Jm=ye(V({},vc),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=Or(!1);try{_c(this)}finally{Or(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=j(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],j(e)}}}),cb=ye(V({},Jm),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(un(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),lb=ye(V({},Jm),{consumerMarkedDirty(){this.view[O]|=8192,ii(this.view),this.notifier.notify(13)},destroy(){if(un(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[hn]?.delete(this)}});function db(e,n,t){let i=Object.create(lb);return i.view=e,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=ep(i,t),e[hn]??=new Set,e[hn].add(i),i.consumerMarkedDirty(i),i}function ub(e,n,t){let i=Object.create(cb);return i.fn=ep(i,e),i.scheduler=n,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function ep(e,n){return()=>{n(t=>(e.cleanupFns??=[]).push(t))}}function En(e){return typeof e=="function"&&e[Ie]!==void 0}var Ps=(()=>{class e{internalPendingTasks=m(ri);scheduler=m(St);errorHandler=m(Kt);add(){let t=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(t)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(t))}}run(t){let i=this.add();try{t().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=X({token:e,providedIn:"root",factory:()=>new e})}return e})();function Xr(e){return{toString:e}.toString()}var $s=class{previousValue;currentValue;firstChange;constructor(n,t,i){this.previousValue=n,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function Op(e,n,t,i){n!==null?n.applyValueToInputSignal(n,i):e[t]=i}var Lt=(()=>{let e=()=>Rp;return e.ngInherit=!0,e})();function Rp(e){return e.type.prototype.ngOnChanges&&(e.setInput=Mb),Ib}function Ib(){let e=Np(this),n=e?.current;if(n){let t=e.previous;if(t===vn)e.previous=n;else for(let i in n)t[i]=n[i];e.current=null,this.ngOnChanges(n)}}function Mb(e,n,t,i,r){let o=this.declaredInputs[i],s=Np(e)||Sb(e,{previous:vn,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new $s(l&&l.currentValue,t,c===vn),Op(e,n,r,t)}var $l="__ngSimpleChanges__";function Np(e){return Object.hasOwn(e,$l)&&e[$l]||null}function Sb(e,n){return e[$l]=n}var tp=[];var oe=function(e,n=null,t){for(let i=0;i<tp.length;i++){let r=tp[i];r(e,n,t)}},ne=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(ne||{});function kb(e,n,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=Rp(n);(t.preOrderHooks??=[]).push(e,s),(t.preOrderCheckHooks??=[]).push(e,s)}r&&(t.preOrderHooks??=[]).push(0-e,r),o&&((t.preOrderHooks??=[]).push(e,o),(t.preOrderCheckHooks??=[]).push(e,o))}function Fp(e,n){for(let t=n.directiveStart,i=n.directiveEnd;t<i;t++){let o=e.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(e.contentHooks??=[]).push(-t,s),a&&((e.contentHooks??=[]).push(t,a),(e.contentCheckHooks??=[]).push(t,a)),c&&(e.viewHooks??=[]).push(-t,c),l&&((e.viewHooks??=[]).push(t,l),(e.viewCheckHooks??=[]).push(t,l)),d!=null&&(e.destroyHooks??=[]).push(t,d)}}function js(e,n,t){Pp(e,n,3,t)}function zs(e,n,t,i){(e[O]&3)===t&&Pp(e,n,t,i)}function Nl(e,n){let t=e[O];(t&3)===n&&(t&=16383,t+=1,e[O]=t)}function Pp(e,n,t,i){let r=i!==void 0?e[Jn]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(e[Jn]+=65536),(a<o||o==-1)&&(Tb(e,t,n,c),e[Jn]=(e[Jn]&4294901760)+c+2),c++}function np(e,n){oe(ne.LifecycleHookStart,e,n);let t=j(null);try{n.call(e)}finally{j(t),oe(ne.LifecycleHookEnd,e,n)}}function Tb(e,n,t,i){let r=t[i]<0,o=t[i+1],s=r?-t[i]:t[i],a=e[s];r?e[O]>>14<e[Jn]>>16&&(e[O]&3)===n&&(e[O]+=16384,np(a,o)):np(a,o)}var tr=-1,li=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,t,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function Ab(e){return(e.flags&8)!==0}function Ob(e){return(e.flags&16)!==0}function Rb(e,n,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let o=t[i++],s=t[i++],a=t[i++];e.setAttribute(n,s,a,o)}else{let o=r,s=t[++i];Nb(o)?e.setProperty(n,o,s):e.setAttribute(n,o,s),i++}}return i}function Lp(e){return e===3||e===4||e===6}function Nb(e){return e.charCodeAt(0)===64}function nr(e,n){if(!(n===null||n.length===0))if(e===null||e.length===0)e=n.slice();else{let t=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?ip(e,t,r,null,n[++i]):ip(e,t,r,null,null))}}return e}function ip(e,n,t,i,r){let o=0,s=e.length;if(n===-1)s=-1;else for(;o<e.length;){let a=e[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<e.length;){let a=e[o];if(typeof a=="number")break;if(a===t){r!==null&&(e[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(e.splice(s,0,n),o=s+1),e.splice(o++,0,t),r!==null&&e.splice(o++,0,r)}function Vp(e){return e!==tr}function Ws(e){return e&32767}function Fb(e){return e>>16}function Gs(e,n){let t=Fb(e),i=n;for(;t>0;)i=i[_n],t--;return i}var Wl=!0;function rp(e){let n=Wl;return Wl=e,n}var Pb=256,Bp=Pb-1,jp=5,Lb=0,Ft={};function Vb(e,n,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Qn)&&(i=t[Qn]),i==null&&(i=t[Qn]=Lb++);let r=i&Bp,o=1<<r;n.data[e+(r>>jp)]|=o}function qs(e,n){let t=zp(e,n);if(t!==-1)return t;let i=n[E];i.firstCreatePass&&(e.injectorIndex=n.length,Fl(i.data,e),Fl(n,null),Fl(i.blueprint,null));let r=wd(e,n),o=e.injectorIndex;if(Vp(r)){let s=Ws(r),a=Gs(r,n),c=a[E].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function Fl(e,n){e.push(0,0,0,0,0,0,0,0,n)}function zp(e,n){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||n[e.injectorIndex+8]===null?-1:e.injectorIndex}function wd(e,n){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let t=0,i=null,r=n;for(;r!==null;){if(i=Gp(r),i===null)return tr;if(t++,r=r[_n],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return tr}function Gl(e,n,t){Vb(e,n,t)}function Bb(e,n){if(n==="class")return e.classes;if(n==="style")return e.styles;let t=e.attrs;if(t){let i=t.length,r=0;for(;r<i;){let o=t[r];if(Lp(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof t[r]=="string";)r++;else{if(o===n)return t[r+1];r=r+2}}}return null}function Hp(e,n,t){if(t&8||e!==void 0)return e;_s(n,"NodeInjector")}function Up(e,n,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=e[Gt],o=Je(void 0);try{return r?r.get(n,i,t&8):tl(n,i,t&8)}finally{Je(o)}}return Hp(i,n,t)}function $p(e,n,t,i=0,r){if(e!==null){if(n[O]&2048&&!(i&2)){let s=Ub(e,n,t,i,Ft);if(s!==Ft)return s}let o=Wp(e,n,t,i,Ft);if(o!==Ft)return o}return Up(n,t,i,r)}function Wp(e,n,t,i,r){let o=zb(t);if(typeof o=="function"){if(!Il(n,e,i))return i&1?Hp(r,t,i):Up(n,t,i,r);try{let s;if(s=o(i),s==null&&!(i&8))_s(t);else return s}finally{Ml()}}else if(typeof o=="number"){let s=null,a=zp(e,n),c=tr,l=i&1?n[Ye][Be]:null;for((a===-1||i&4)&&(c=a===-1?wd(e,n):n[a+8],c===tr||!sp(i,!1)?a=-1:(s=n[E],a=Ws(c),n=Gs(c,n)));a!==-1;){let d=n[E];if(op(o,a,d.data)){let h=jb(a,n,t,s,i,l);if(h!==Ft)return h}c=n[a+8],c!==tr&&sp(i,n[E].data[a+8]===l)&&op(o,a,n)?(s=d,a=Ws(c),n=Gs(c,n)):a=-1}}return r}function jb(e,n,t,i,r,o){let s=n[E],a=s.data[e+8],c=i==null?Yt(a)&&Wl:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Hs(a,s,t,c,l);return d!==null?qr(n,s,d,a,r):Ft}function Hs(e,n,t,i,r){let o=e.providerIndexes,s=n.data,a=o&1048575,c=e.directiveStart,l=e.directiveEnd,d=o>>20,h=i?a:a+d,v=r?a+d:l;for(let g=h;g<v;g++){let _=s[g];if(g<c&&t===_||g>=c&&_.type===t)return g}if(r){let g=s[c];if(g&&At(g)&&g.type===t)return c}return null}function qr(e,n,t,i,r){let o=e[t],s=n.data;if(o instanceof li){let a=o;if(a.resolving)throw el("");let c=rp(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],d,h=a.injectImpl?Je(a.injectImpl):null,v=Il(e,i,0);try{o=e[t]=a.factory(void 0,r,s,e,i),n.firstCreatePass&&t>=i.directiveStart&&kb(t,s[t],n)}finally{h!==null&&Je(h),rp(c),a.resolving=!1,Ml()}}return o}function zb(e){if(typeof e=="string")return e.charCodeAt(0)||0;let n=e.hasOwnProperty(Qn)?e[Qn]:void 0;return typeof n=="number"?n>=0?n&Bp:Hb:n}function op(e,n,t){let i=1<<e;return!!(t[n+(e>>jp)]&i)}function sp(e,n){return!(e&2)&&!(e&1&&n)}var In=class{_tNode;_lView;constructor(n,t){this._tNode=n,this._lView=t}get(n,t,i){return $p(this._tNode,this._lView,n,qn(i),t)}};function Hb(){return new In(Ne(),q())}function ke(e){return Xr(()=>{let n=e.prototype.constructor,t=n[Ar]||ql(n),i=Object.prototype,r=Object.getPrototypeOf(e.prototype).constructor;for(;r&&r!==i;){let o=r[Ar]||ql(r);if(o&&o!==t)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function ql(e){return Gc(e)?()=>{let n=ql(Ae(e));return n&&n()}:Zn(e)}function Ub(e,n,t,i,r){let o=e,s=n;for(;o!==null&&s!==null&&s[O]&2048&&!Ki(s);){let a=Wp(o,s,t,i|2,Ft);if(a!==Ft)return a;let c=o.parent;if(!c){let l=s[cl];if(l){let d=l.get(t,Ft,i&-5);if(d!==Ft)return d}c=Gp(s),s=s[_n]}o=c}return r}function Gp(e){let n=e[E],t=n.type;return t===2?n.declTNode:t===1?e[Be]:null}function xd(e){return Bb(Ne(),e)}function L(e){return{token:e.token,providedIn:e.autoProvided===!1?null:"root",factory:e.factory,value:void 0}}function $b(){return sr(Ne(),q())}function sr(e,n){return new B(gt(e,n))}var B=(()=>{class e{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=$b}return e})();function qp(e){return e instanceof B?e.nativeElement:e}function Wb(){return this._results[Symbol.iterator]()}var bt=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new w}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,t){return this._results.reduce(n,t)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,t){this.dirty=!1;let i=hm(n);(this._changesDetected=!pm(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Wb};function Zp(e){return(e.flags&128)===128}var Cd=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(Cd||{}),Yp=new Map,Gb=0;function qb(){return Gb++}function Zb(e){Yp.set(e[qt],e)}function Zl(e){Yp.delete(e[qt])}var ap="__ngContext__";function ir(e,n){Zt(n)?(e[ap]=n[qt],Zb(n)):e[ap]=n}function Kp(e){return Xp(e[Yi])}function Qp(e){return Xp(e[it])}function Xp(e){for(;e!==null&&!rt(e);)e=e[it];return e}var Yl;function Dd(e){Yl=e}function Jp(){if(Yl!==void 0)return Yl;if(typeof document<"u")return document;throw new D(210,!1)}var eh=!1,th=new y("",{factory:()=>eh});var cp=new WeakMap;function Yb(e,n){if(e==null||typeof e!="object")return;let t=cp.get(e);t||(t=new WeakSet,cp.set(e,t)),t.add(n)}var Kb=(e,n,t,i)=>{};function Qb(e,n,t,i){Kb(e,n,t,i)}function na(e){return(e.flags&32)===32}var Xb=()=>null;function nh(e,n,t=!1){return Xb(e,n,t)}function ih(e,n){let t=e.contentQueries;if(t!==null){let i=j(null);try{for(let r=0;r<t.length;r+=2){let o=t[r],s=t[r+1];if(s!==-1){let a=e.data[s];zr(o),a.contentQueries(2,n[s],s)}}}finally{j(i)}}}function Kl(e,n,t){zr(0);let i=j(null);try{n(e,t)}finally{j(i)}}function rh(e,n,t){if(ll(n)){let i=j(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=e.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{j(i)}}}var wt=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(wt||{});var Ls;function Jb(){if(Ls===void 0&&(Ls=null,Hi.trustedTypes))try{Ls=Hi.trustedTypes.createPolicy("angular",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch{}return Ls}function ia(e){return Jb()?.createHTML(e)||e}var Qt=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${ms})`}},Ql=class extends Qt{getTypeName(){return"HTML"}},Xl=class extends Qt{getTypeName(){return"Style"}},Jl=class extends Qt{getTypeName(){return"Script"}},ed=class extends Qt{getTypeName(){return"URL"}},td=class extends Qt{getTypeName(){return"ResourceURL"}};function Jt(e){return e instanceof Qt?e.changingThisBreaksApplicationSecurity:e}function mi(e,n){let t=oh(e);if(t!=null&&t!==n){if(t==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${t} (see ${ms})`)}return t===n}function oh(e){return e instanceof Qt&&e.getTypeName()||null}function Ed(e){return new Ql(e)}function Id(e){return new Xl(e)}function Md(e){return new Jl(e)}function Sd(e){return new ed(e)}function kd(e){return new td(e)}function e0(e){let n=new id(e);return t0()?new nd(n):n}var nd=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let t=new window.DOMParser().parseFromString(ia(n),"text/html").body;return t===null?this.inertDocumentHelper.getInertBodyElement(n):(t.firstChild?.remove(),t)}catch{return null}}},id=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let t=this.inertDocument.createElement("template");return t.innerHTML=ia(n),t}};function t0(){try{return!!new window.DOMParser().parseFromString(ia(""),"text/html")}catch{return!1}}var n0=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function ra(e){return e=String(e),e.match(n0)?e:"unsafe:"+e}function en(e){let n={};for(let t of e.split(","))n[t]=!0;return n}function Jr(...e){let n={};for(let t of e)for(let i in t)t.hasOwnProperty(i)&&(n[i]=!0);return n}var sh=en("area,br,col,hr,img,wbr"),ah=en("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),ch=en("rp,rt"),i0=Jr(ch,ah),r0=Jr(ah,en("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),o0=Jr(ch,en("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),lp=Jr(sh,r0,o0,i0),lh=en("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),s0=en("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),a0=en("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),c0=Jr(lh,s0,a0),l0=en("script,style,template"),rd=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let t=n.firstChild,i=!0,r=[];for(;t;){if(t.nodeType===Node.ELEMENT_NODE?i=this.startElement(t):t.nodeType===Node.TEXT_NODE?this.chars(t.nodeValue):this.sanitizedSomething=!0,i&&t.firstChild){r.push(t),t=f0(t);continue}for(;t;){t.nodeType===Node.ELEMENT_NODE&&this.endElement(t);let o=u0(t);if(o){t=o;break}t=r.pop()}}return this.buf.join("")}startElement(n){let t=dp(n).toLowerCase();if(!lp.hasOwnProperty(t))return this.sanitizedSomething=!0,!l0.hasOwnProperty(t);this.buf.push("<"),this.buf.push(t);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!c0.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;lh[a]&&(c=ra(c)),this.buf.push(" ",s,'="',up(c),'"')}return this.buf.push(">"),!0}endElement(n){let t=dp(n).toLowerCase();lp.hasOwnProperty(t)&&!sh.hasOwnProperty(t)&&(this.buf.push("</"),this.buf.push(t),this.buf.push(">"))}chars(n){this.buf.push(up(n))}};function d0(e,n){return(e.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function u0(e){let n=e.nextSibling;if(n&&e!==n.previousSibling)throw dh(n);return n}function f0(e){let n=e.firstChild;if(n&&d0(e,n))throw dh(n);return n}function dp(e){let n=e.nodeName;return typeof n=="string"?n:"FORM"}function dh(e){return new Error(`Failed to sanitize html because the element is clobbered: ${e.outerHTML}`)}var m0=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,p0=/([^\#-~ |!])/g;function up(e){return e.replace(/&/g,"&amp;").replace(m0,function(n){let t=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((t-55296)*1024+(i-56320)+65536)+";"}).replace(p0,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Vs;function Td(e,n){let t=null;try{Vs=Vs||e0(e);let i=n?String(n):"";t=Vs.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=t.innerHTML,t=Vs.getInertBodyElement(i)}while(i!==o);let a=new rd().sanitizeChildren(fp(t)||t);return ia(a)}finally{if(t){let i=fp(t)||t;for(;i.firstChild;)i.firstChild.remove()}}}function fp(e){return"content"in e&&h0(e)?e.content:null}function h0(e){return e.nodeType===Node.ELEMENT_NODE&&e.nodeName==="TEMPLATE"}function g0(e,n){return e.createText(n)}function v0(e,n,t){e.setValue(n,t)}function uh(e,n,t){return e.createElement(n,t)}function Zs(e,n,t,i,r){e.insertBefore(n,t,i,r)}function fh(e,n,t){e.appendChild(n,t)}function mp(e,n,t,i,r){i!==null?Zs(e,n,t,i,r):fh(e,n,t)}function _0(e,n,t,i){e.removeChild(null,n,t,i)}function y0(e,n,t){e.setAttribute(n,"style",t)}function b0(e,n,t){t===""?e.removeAttribute(n,"class"):e.setAttribute(n,"class",t)}function mh(e,n,t){let{mergedAttrs:i,classes:r,styles:o}=t;i!==null&&Rb(e,n,i),r!==null&&b0(e,n,r),o!==null&&y0(e,n,o)}var We=(function(e){return e[e.NONE=0]="NONE",e[e.HTML=1]="HTML",e[e.STYLE=2]="STYLE",e[e.SCRIPT=3]="SCRIPT",e[e.URL=4]="URL",e[e.RESOURCE_URL=5]="RESOURCE_URL",e[e.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",e})(We||{});function w0(e,n,t){let i=e.length;for(;;){let r=e.indexOf(n,t);if(r===-1)return r;if(r===0||e.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||e.charCodeAt(r+o)<=32)return r}t=r+1}}var ph="ng-template";function x0(e,n,t,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&w0(n[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Ad(e))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===t)return!0}return!1}function Ad(e){return e.type===4&&e.value!==ph}function C0(e,n,t){let i=e.type===4&&!t?ph:e.value;return n===i}function D0(e,n,t){let i=4,r=e.attrs,o=r!==null?M0(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!_t(i)&&!_t(c))return!1;if(s&&_t(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!C0(e,c,t)||c===""&&n.length===1){if(_t(i))return!1;s=!0}}else if(i&8){if(r===null||!x0(e,r,c,t)){if(_t(i))return!1;s=!0}}else{let l=n[++a],d=E0(c,r,Ad(e),t);if(d===-1){if(_t(i))return!1;s=!0;continue}if(l!==""){let h;if(d>o?h="":h=r[d+1].toLowerCase(),i&2&&l!==h){if(_t(i))return!1;s=!0}}}}return _t(i)||s}function _t(e){return(e&1)===0}function E0(e,n,t,i){if(n===null)return-1;let r=0;if(i||!t){let o=!1;for(;r<n.length;){let s=n[r];if(s===e)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return S0(n,e)}function hh(e,n,t=!1){for(let i=0;i<n.length;i++)if(D0(e,n[i],t))return!0;return!1}function I0(e){let n=e.attrs;if(n!=null){let t=n.indexOf(5);if((t&1)===0)return n[t+1]}return null}function M0(e){for(let n=0;n<e.length;n++){let t=e[n];if(Lp(t))return n}return e.length}function S0(e,n){let t=e.indexOf(4);if(t>-1)for(t++;t<e.length;){let i=e[t];if(typeof i=="number")return-1;if(i===n)return t;t++}return-1}function k0(e,n){e:for(let t=0;t<n.length;t++){let i=n[t];if(e.length===i.length){for(let r=0;r<e.length;r++)if(e[r]!==i[r])continue e;return!0}}return!1}function pp(e,n){return e?":not("+n.trim()+")":n}function T0(e){let n=e[0],t=1,i=2,r="",o=!1;for(;t<e.length;){let s=e[t];if(typeof s=="string")if(i&2){let a=e[++t];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!_t(s)&&(n+=pp(o,r),r=""),i=s,o=o||!_t(i);t++}return r!==""&&(n+=pp(o,r)),n}function A0(e){return e.map(T0).join(",")}function O0(e){let n=[],t=[],i=1,r=2;for(;i<e.length;){let o=e[i];if(typeof o=="string")r===2?o!==""&&n.push(o,e[++i]):r===8&&t.push(o);else{if(!_t(r))break;r=o}i++}return t.length&&n.push(1,...t),n}var xt={},Pt=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(Pt||{}),R0;function Od(e,n){return R0(e,n)}var VR=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var od=new WeakMap;function gh(e){return e?e[_n]??e:null}var $r=new WeakSet;function N0(e,n,t){let i=od.get(e);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=gh(t);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],d=c.parentNode;c===n?(i.splice(a,1),$r.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&r&&d!==r&&(s===null||l===null||s===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function F0(e,n,t){let i=gh(t),r=od.get(e);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):od.set(e,[{el:n,declarationView:i}])}var di=new Set,oa=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(oa||{}),Vt=new y(""),hp=new Set;function ar(e){hp.has(e)||(hp.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var sa=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=X({token:e,providedIn:"root",factory:()=>new e})}return e})(),Rd=[0,1,2,3],Nd=(()=>{class e{ngZone=m(C);scheduler=m(St);errorHandler=m(Ze,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){m(Vt,{optional:!0})}execute(){let t=this.sequences.size>0;t&&oe(ne.AfterRenderHooksStart),this.executing=!0;for(let i of Rd)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&oe(ne.AfterRenderHooksEnd)}register(t){let{view:i}=t;i!==void 0?((i[ei]??=[]).push(t),ii(i),i[O]|=8192):this.executing?this.deferredRegistrations.add(t):this.addSequence(t)}addSequence(t){this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,i){return i?i.run(oa.AFTER_NEXT_RENDER,t):t()}static \u0275prov=X({token:e,providedIn:"root",factory:()=>new e})}return e})(),Zr=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,t,i,r,o,s=null){this.impl=n,this.hooks=t,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[ei];n&&(this.view[ei]=n.filter(t=>t!==this))}};function ct(e,n){let t=n?.injector??m(ee);return ar("NgAfterNextRender"),L0(e,t,n,!0)}function P0(e){return e instanceof Function?[void 0,void 0,e,void 0]:[e.earlyRead,e.write,e.mixedReadWrite,e.read]}function L0(e,n,t,i){let r=n.get(sa);r.impl??=n.get(Nd);let o=n.get(Vt,null,{optional:!0}),s=t?.manualCleanup!==!0?n.get(at):null,a=n.get(Ji,null,{optional:!0}),c=new Zr(r.impl,P0(e),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var vh=new y("",{factory:()=>{let e=m(be),n=new Set;return e.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:e}}});function _h(e,n,t){let i=e.get(vh);if(Array.isArray(n))for(let r of n)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),t?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(e)}function V0(e,n){let t=e.get(vh);if(Array.isArray(n))for(let i of n)t.queue.delete(i);else t.queue.delete(n)}function B0(e,n){for(let[t,i]of n)_h(e,i.animateFns)}function gp(e,n,t,i){let r=e?.[bn]?.enter;n!==null&&r&&r.has(t.index)&&B0(i,r)}function vp(e,n,t,i){try{t.get(Pr)}catch{return i(!1)}let r=e?.[bn];r?.enter?.has(n.index)&&V0(t,r.enter.get(n.index).animateFns);let o=j0(e,n,r);if(o.size===0){let s=!1;if(e){let a=[];aa(e,n,a),s=a.length>0}if(!s)return i(!1)}e&&di.add(e[qt]),_h(t,()=>z0(e,n,r||void 0,o,i),r||void 0)}function j0(e,n,t){let i=new Map,r=t?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),e&&r)for(let[o,s]of r){if(i.has(o))continue;let c=e[E].data[o].parent;for(;c;){if(c===n){i.set(o,s);break}c=c.parent}}return i}function z0(e,n,t,i,r){let o=[];if(t&&t.leave)for(let[s]of i){if(!t.leave.has(s))continue;let a=t.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}t.detachedLeaveAnimationFns=void 0}if(e&&aa(e,n,o),o.length>0){let s=t||e?.[bn];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),U0(e,s.running,r)}else Promise.allSettled(o).then(()=>{e&&di.delete(e[qt]),r(!0)})}else e&&di.delete(e[qt]),r(!1)}function aa(e,n,t){if(n.type&12){let r=e[n.index];if(rt(r))for(let o=Re;o<r.length;o++){let s=r[o];s[E].type===2&&H0(s,t)}}let i=n.child;for(;i;)aa(e,i,t),i=i.next}function H0(e,n){let t=e[bn];if(t&&t.leave)for(let r of t.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=e[E].firstChild;for(;i;)aa(e,i,n),i=i.next}function U0(e,n,t){n.then(()=>{e[bn]?.running===n&&(e[bn].running=void 0,di.delete(e[qt])),t(!0)})}function er(e,n,t,i,r,o,s,a){if(r!=null){let c,l=!1;rt(r)?c=r:Zt(r)&&(l=!0,r=r[ht]);let d=ot(r);e===0&&i!==null?(gp(a,i,o,t),s==null?fh(n,i,d):Zs(n,i,d,s||null,!0)):e===1&&i!==null?(gp(a,i,o,t),Zs(n,i,d,s||null,!0),N0(o,d,a)):e===2?(a?.[bn]?.leave?.has(o.index)&&F0(o,d,a),$r.delete(d),vp(a,o,t,h=>{if($r.has(d)){$r.delete(d);return}_0(n,d,l,h)})):e===3&&($r.delete(d),vp(a,o,t,()=>{n.destroyNode(d)})),c!=null&&J0(n,e,t,c,o,i,s)}}function $0(e,n){yh(e,n),n[ht]=null,n[Be]=null}function W0(e,n,t,i,r,o){i[ht]=r,i[Be]=n,ca(e,i,t,1,r,o)}function yh(e,n){n[kt].changeDetectionScheduler?.notify(9),ca(e,n,n[ge],2,null,null)}function G0(e){let n=e[Yi];if(!n)return Pl(e[E],e);for(;n;){let t=null;if(Zt(n))t=n[Yi];else{let i=n[Re];i&&(t=i)}if(!t){for(;n&&!n[it]&&n!==e;)Zt(n)&&Pl(n[E],n),n=n[Me];n===null&&(n=e),Zt(n)&&Pl(n[E],n),t=n&&n[it]}n=t}}function Fd(e,n){let t=e[ti],i=t.indexOf(n);t.splice(i,1)}function Pd(e,n){if(ni(n))return;let t=n[ge];t.destroyNode&&ca(e,n,t,3,null,null),G0(n)}function Pl(e,n){if(ni(n))return;let t=j(null);try{n[O]&=-129,n[O]|=256,n[et]&&un(n[et]),Z0(e,n),q0(e,n),n[E].type===1&&n[ge].destroy();let i=n[yn];if(i!==null&&rt(n[Me])){i!==n[Me]&&Fd(i,n);let r=n[Tt];r!==null&&r.detachView(e)}Zl(n)}finally{j(t)}}function q0(e,n){let t=e.cleanup,i=n[Zi];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[t[s+1]];t[s].call(a)}i!==null&&(n[Zi]=null);let r=n[Ut];if(r!==null){n[Ut]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[hn];if(o!==null){n[hn]=null;for(let s of o)s.destroy()}}function Z0(e,n){let t;if(e!=null&&(t=e.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=n[t[i]];if(!(r instanceof li)){let o=t[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];oe(ne.LifecycleHookStart,a,c);try{c.call(a)}finally{oe(ne.LifecycleHookEnd,a,c)}}else{oe(ne.LifecycleHookStart,r,o);try{o.call(r)}finally{oe(ne.LifecycleHookEnd,r,o)}}}}}function bh(e,n,t){return Y0(e,n.parent,t)}function Y0(e,n,t){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return t[ht];if(Yt(i)){let{encapsulation:r}=e.data[i.directiveStart+i.componentOffset];if(r===wt.None||r===wt.Emulated)return null}return gt(i,t)}function wh(e,n,t){return Q0(e,n,t)}function K0(e,n,t){return e.type&40?gt(e,t):null}var Q0=K0,_p;function Ld(e,n,t,i){let r=bh(e,i,n),o=n[ge],s=i.parent||n[Be],a=wh(s,i,n);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)mp(o,r,t[c],a,!1);else mp(o,r,t,a,!1);_p!==void 0&&_p(o,i,n,t,r)}function Wr(e,n){if(n!==null){let t=n.type;if(t&3)return gt(n,e);if(t&4)return sd(-1,e[n.index]);if(t&8){let i=n.child;if(i!==null)return Wr(e,i);{let r=e[n.index];return rt(r)?sd(-1,r):ot(r)}}else{if(t&128)return Wr(e,n.next);if(t&32)return Od(n,e)()||ot(e[n.index]);{let i=xh(e,n);if(i!==null){if(Array.isArray(i))return i[0];let r=$t(e[Ye]);return Wr(r,i)}else return Wr(e,n.next)}}}return null}function xh(e,n){if(n!==null){let i=e[Ye][Be],r=n.projection;return i.projection[r]}return null}function sd(e,n){let t=Re+e+1;if(t<n.length){let i=n[t],r=i[E].firstChild;if(r!==null)return Wr(i,r)}return n[wn]}function Vd(e,n,t,i,r,o,s){for(;t!=null;){let a=i[Gt];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(s&&n===0&&(c&&ir(ot(c),i),t.flags|=2),!na(t))if(l&8)Vd(e,n,t.child,i,r,o,!1),er(n,e,a,r,c,t,o,i);else if(l&32){let d=Od(t,i),h;for(;h=d();)er(n,e,a,r,h,t,o,i);er(n,e,a,r,c,t,o,i)}else l&16?Ch(e,n,i,t,r,o):er(n,e,a,r,c,t,o,i);t=s?t.projectionNext:t.next}}function ca(e,n,t,i,r,o){Vd(t,i,e.firstChild,n,r,o,!1)}function X0(e,n,t){let i=n[ge],r=bh(e,t,n),o=t.parent||n[Be],s=wh(o,t,n);Ch(i,0,n,t,r,s)}function Ch(e,n,t,i,r,o){let s=t[Ye],c=s[Be].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];er(n,e,t[Gt],r,d,i,o,t)}else{let l=c,d=s[Me];Zp(i)&&(l.flags|=128),Vd(e,n,l,d,r,o,!0)}}function J0(e,n,t,i,r,o,s){let a=i[wn],c=ot(i);a!==c&&er(n,e,t,o,a,r,s);for(let l=Re;l<i.length;l++){let d=i[l];ca(d[E],d,e,n,o,a)}}function ew(e,n,t,i,r){if(n)r?e.addClass(t,i):e.removeClass(t,i);else{let o=i.indexOf("-")===-1?void 0:Pt.DashCase;r==null?e.removeStyle(t,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Pt.Important),e.setStyle(t,i,r,o))}}function Bd(e,n,t,i,r,o,s,a,c,l,d){let h=xe+i,v=h+r,g=tw(h,v),_=typeof l=="function"?l():l;return g[E]={type:e,blueprint:g,template:t,queries:null,viewQuery:a,declTNode:n,data:g.slice().fill(null,h),bindingStartIndex:h,expandoStartIndex:v,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:_,incompleteFirstPass:!1,ssrId:d}}function tw(e,n){let t=[];for(let i=0;i<n;i++)t.push(i<e?null:xt);return t}function nw(e){let n=e.tView;return n===null||n.incompleteFirstPass?e.tView=Bd(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):n}function jd(e,n,t,i,r,o,s,a,c,l,d){let h=n.blueprint.slice();return h[ht]=r,h[O]=i|4|128|8|64|1024,(l!==null||e&&e[O]&2048)&&(h[O]|=2048),fl(h),h[Me]=h[_n]=e,h[Oe]=t,h[kt]=s||e&&e[kt],h[ge]=a||e&&e[ge],h[Gt]=c||e&&e[Gt]||null,h[Be]=o,h[qt]=qb(),h[qi]=d,h[cl]=l,h[Ye]=n.type==2?e[Ye]:h,h}function iw(e,n,t){let i=gt(n,e),r=nw(t),o=e[kt].rendererFactory,s=zd(e,jd(e,r,null,Dh(t),i,n,null,o.createRenderer(i,t),null,null,null));return e[n.index]=s}function Dh(e){let n=16;return e.signals?n=4096:e.onPush&&(n=64),n}function Eh(e,n,t,i){if(t===0)return-1;let r=n.length;for(let o=0;o<t;o++)n.push(i),e.blueprint.push(i),e.data.push(null);return r}function zd(e,n){return e[Yi]?e[al][it]=n:e[Yi]=n,e[al]=n,n}function z(e=1){Ih(De(),q(),xn()+e,!1)}function Ih(e,n,t,i){if(!i)if((n[O]&3)===3){let o=e.preOrderCheckHooks;o!==null&&js(n,o,t)}else{let o=e.preOrderHooks;o!==null&&zs(n,o,0,t)}Cn(t)}var la=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(la||{});function ad(e,n,t,i){let r=j(null);try{let[o,s,a]=e.inputs[t],c=null;(s&la.SignalBased)!==0&&(c=n[o][Ie]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),e.setInput!==null?e.setInput(n,c,i,t,o):Op(n,c,o,i)}finally{j(r)}}function Mh(e,n,t,i,r){let o=xn(),s=i&2;try{Cn(-1),s&&n.length>xe&&Ih(e,n,xe,!1);let a=s?ne.TemplateUpdateStart:ne.TemplateCreateStart;oe(a,r,t),t(i,r)}finally{Cn(o);let a=s?ne.TemplateUpdateEnd:ne.TemplateCreateEnd;oe(a,r,t)}}function Hd(e,n,t){dw(e,n,t),(t.flags&64)===64&&uw(e,n,t)}function da(e,n,t=gt){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?t(n,e):e[s];e[r++]=a}}}function rw(e,n,t,i){let o=i.get(th,eh)||t===wt.ShadowDom||t===wt.ExperimentalIsolatedShadowDom,s=e.selectRootElement(n,o);if(s.tagName.toLowerCase()==="script")throw new D(905,!1);return ow(s),s}function ow(e){sw(e)}var sw=()=>null;function aw(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function cw(e,n,t,i,r,o){let s=n[E];if(Ud(e,s,n,t,i)){Yt(e)&&lw(n,e.index);return}e.type&3&&(t=aw(t)),Sh(e,n,t,i,r,o)}function Sh(e,n,t,i,r,o){if(e.type&3){let s=gt(e,n);i=o!=null?o(i,e.value||"",t):i,r.setProperty(s,t,i)}else e.type&12}function lw(e,n){let t=st(n,e);t[O]&16||(t[O]|=64)}function dw(e,n,t){let i=t.directiveStart,r=t.directiveEnd;Yt(t)&&iw(n,t,e.data[i+t.componentOffset]),e.firstCreatePass||qs(t,n);let o=t.initialInputs;for(let s=i;s<r;s++){let a=e.data[s],c=qr(n,e,s,t);if(ir(c,n),o!==null&&hw(n,s-i,c,a,t,o),At(a)){let l=st(t.index,n);l[Oe]=qr(n,e,s,t)}}}function uw(e,n,t){let i=t.directiveStart,r=t.directiveEnd,o=t.index,s=Bm();try{Cn(o);for(let a=i;a<r;a++){let c=e.data[a],l=n[a];Ms(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&fw(c,l)}}finally{Cn(-1),Ms(s)}}function fw(e,n){e.hostBindings!==null&&e.hostBindings(1,n)}function kh(e,n){let t=e.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let o=t[r];hh(n,o.selectors,!1)&&(i??=[],At(o)?i.unshift(o):i.push(o))}return i}function mw(e,n,t,i,r,o){let s=gt(e,n);pw(n[ge],s,o,e.value,t,i,r)}function pw(e,n,t,i,r,o,s){if(o==null)s?.(o,i||"",r),e.removeAttribute(n,r,t);else{let a=s==null?Jc(o):s(o,i||"",r);e.setAttribute(n,r,a,t)}}function hw(e,n,t,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];ad(i,t,c,l)}}function Th(e,n,t,i,r){let o=xe+t,s=n[E],a=r(s,n,e,i,t);n[o]=a,Qi(e,!0);let c=e.type===2;return c?(mh(n[ge],a,e),(Om()===0||Br(e))&&ir(a,n),Rm()):ir(a,n),Os()&&(!c||!na(e))&&Ld(s,n,a,e),e}function Ah(e){let n=e;return xl()?Cl():(n=n.parent,Qi(n,!1)),n}function gw(e,n){let t=e[Gt];if(!t)return;let i;try{i=t.get(Kt,null)}catch{i=null}i?.(n)}function Ud(e,n,t,i,r){let o=e.inputs?.[i],s=e.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],h=n.data[l];ad(h,t[l],d,r),a=!0}if(o)for(let c of o){let l=t[c],d=n.data[c];ad(d,l,i,r),a=!0}return a}function vw(e,n){let t=st(n,e),i=t[E];_w(i,t);let r=t[ht];r!==null&&t[qi]===null&&(t[qi]=nh(r,t[Gt])),oe(ne.ComponentStart);try{$d(i,t,t[Oe])}finally{oe(ne.ComponentEnd,t[Oe])}}function _w(e,n){for(let t=n.length;t<e.blueprint.length;t++)n.push(e.blueprint[t])}function $d(e,n,t){ks(n);try{let i=e.viewQuery;i!==null&&Kl(1,i,t);let r=e.template;r!==null&&Mh(e,n,r,1,t),e.firstCreatePass&&(e.firstCreatePass=!1),n[Tt]?.finishViewCreation(e),e.staticContentQueries&&ih(e,n),e.staticViewQueries&&Kl(2,e.viewQuery,t);let o=e.components;o!==null&&yw(n,o)}catch(i){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),i}finally{n[O]&=-5,Ts()}}function yw(e,n){for(let t=0;t<n.length;t++)vw(e,n[t])}function Wd(e,n,t,i){let r=j(null);try{let o=n.tView,a=e[O]&4096?4096:16,c=jd(e,o,t,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=e[n.index];c[yn]=l;let d=e[Tt];return d!==null&&(c[Tt]=d.createEmbeddedView(o)),$d(o,c,t),c}finally{j(r)}}function Ys(e,n){return!n||n.firstChild===null||Zp(e)}function Yr(e,n,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let o=n[t.index];o!==null&&i.push(ot(o)),rt(o)&&Oh(o,i);let s=t.type;if(s&8)Yr(e,n,t.child,i);else if(s&32){let a=Od(t,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=xh(n,t);if(Array.isArray(a))i.push(...a);else{let c=$t(n[Ye]);Yr(c[E],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function Oh(e,n){for(let t=Re;t<e.length;t++){let i=e[t],r=i[E].firstChild;r!==null&&Yr(i[E],i,r,n)}e[wn]!==e[ht]&&n.push(e[wn])}function Rh(e){if(e[ei]!==null){for(let n of e[ei])n.impl.addSequence(n);e[ei].length=0}}var Nh=[];function bw(e){return e[et]??ww(e)}function ww(e){let n=Nh.pop()??Object.create(Cw);return n.lView=e,n}function xw(e){e.lView[et]!==e&&(e.lView=null,Nh.push(e))}var Cw=ye(V({},Mi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{ii(e.lView)},consumerOnSignalRead(){this.lView[et]=this}});function Dw(e){let n=e[et]??Object.create(Ew);return n.lView=e,n}var Ew=ye(V({},Mi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let n=$t(e.lView);for(;n&&!Fh(n[E]);)n=$t(n);n&&ml(n)},consumerOnSignalRead(){this.lView[et]=this}});function Fh(e){return e.type!==2}function Ph(e){if(e[hn]===null)return;let n=!0;for(;n;){let t=!1;for(let i of e[hn])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=t&&!!(e[O]&8192)}}var Iw=100;function Lh(e,n=0){let i=e[kt].rendererFactory,r=!1;r||i.begin?.();try{Mw(e,n)}finally{r||i.end?.()}}function Mw(e,n){let t=Dl();try{Or(!0),cd(e,n);let i=0;for(;jr(e);){if(i===Iw)throw new D(103,!1);i++,cd(e,1)}}finally{Or(t)}}function Sw(e,n,t,i){if(ni(n))return;let r=n[O],o=!1,s=!1;ks(n);let a=!0,c=null,l=null;o||(Fh(e)?(l=bw(n),c=Pn(l)):So()===null?(a=!1,l=Dw(n),c=Pn(l)):n[et]&&(un(n[et]),n[et]=null));try{fl(n),Pm(e.bindingStartIndex),t!==null&&Mh(e,n,t,2,i);let d=(r&3)===3;if(!o)if(d){let g=e.preOrderCheckHooks;g!==null&&js(n,g,null)}else{let g=e.preOrderHooks;g!==null&&zs(n,g,0,null),Nl(n,0)}if(s||kw(n),Ph(n),Vh(n,0),e.contentQueries!==null&&ih(e,n),!o)if(d){let g=e.contentCheckHooks;g!==null&&js(n,g)}else{let g=e.contentHooks;g!==null&&zs(n,g,1),Nl(n,1)}Aw(e,n);let h=e.components;h!==null&&jh(n,h,0);let v=e.viewQuery;if(v!==null&&Kl(2,v,i),!o)if(d){let g=e.viewCheckHooks;g!==null&&js(n,g)}else{let g=e.viewHooks;g!==null&&zs(n,g,2),Nl(n,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),n[xs]){for(let g of n[xs])g();n[xs]=null}o||(Rh(n),n[O]&=-73)}catch(d){throw o||ii(n),d}finally{l!==null&&(ki(l,c),a&&xw(l)),Ts()}}function Vh(e,n){for(let t=Kp(e);t!==null;t=Qp(t))for(let i=Re;i<t.length;i++){let r=t[i];Bh(r,n)}}function kw(e){for(let n=Kp(e);n!==null;n=Qp(n)){if(!(n[O]&2))continue;let t=n[ti];for(let i=0;i<t.length;i++){let r=t[i];ml(r)}}}function Tw(e,n,t){oe(ne.ComponentStart);let i=st(n,e);try{Bh(i,t)}finally{oe(ne.ComponentEnd,i[Oe])}}function Bh(e,n){Ds(e)&&cd(e,n)}function cd(e,n){let i=e[E],r=e[O],o=e[et],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&Ti(o)),s||=!1,o&&(o.dirty=!1),e[O]&=-9217,s)Sw(i,e,i.template,e[Oe]);else if(r&8192){let a=j(null);try{Ph(e),Vh(e,1);let c=i.components;c!==null&&jh(e,c,1),Rh(e)}finally{j(a)}}}function jh(e,n,t){for(let i=0;i<n.length;i++)Tw(e,n[i],t)}function Aw(e,n){let t=e.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Cn(~r);else{let o=r,s=t[++i],a=t[++i];Vm(s,o);let c=n[o];oe(ne.HostBindingsUpdateStart,c);try{a(2,c)}finally{oe(ne.HostBindingsUpdateEnd,c)}}}}finally{Cn(-1)}}function Gd(e,n){let t=Dl()?64:1088;for(e[kt].changeDetectionScheduler?.notify(n);e;){e[O]|=t;let i=$t(e);if(Ki(e)&&!i)return e;e=i}return null}function zh(e,n,t,i){return[e,!0,0,n,null,i,null,t,null,null]}function Ow(e,n){let t=Re+n;if(t<e.length)return e[t]}function qd(e,n,t,i=!0){let r=n[E];if(Nw(r,n,e,t),i){let s=sd(t,e),a=n[ge],c=a.parentNode(e[wn]);c!==null&&W0(r,e[Be],a,n,c,s)}let o=n[qi];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function Rw(e,n){let t=Ks(e,n);return t!==void 0&&Pd(t[E],t),t}function Ks(e,n){if(e.length<=Re)return;let t=Re+n,i=e[t];if(i){let r=i[yn];r!==null&&r!==e&&Fd(r,i),n>0&&(e[t-1][it]=i[it]);let o=Fr(e,Re+n);$0(i[E],i);let s=o[Tt];s!==null&&s.detachView(o[E]),i[Me]=null,i[it]=null,i[O]&=-129}return i}function Nw(e,n,t,i){let r=Re+i,o=t.length;i>0&&(t[r-1][it]=n),i<o-Re?(n[it]=t[r],nl(t,Re+i,n)):(t.push(n),n[it]=null),n[Me]=t;let s=n[yn];s!==null&&t!==s&&Hh(s,n);let a=n[Tt];a!==null&&a.insertView(e),Es(n),n[O]|=128}function Hh(e,n){let t=e[ti],i=n[Me];if(Zt(i))e[O]|=2;else{let r=i[Me][Ye];n[Ye]!==r&&(e[O]|=2)}t===null?e[ti]=[n]:t.push(n)}var Mn=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,t=n[E];return Yr(t,n,t.firstChild,[])}constructor(n,t){this._lView=n,this._cdRefInjectingView=t}get context(){return this._lView[Oe]}set context(n){this._lView[Oe]=n}get destroyed(){return ni(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Me];if(rt(n)){let t=n[Vr],i=t?t.indexOf(this):-1;i>-1&&(Ks(n,i),Fr(t,i))}this._attachedToViewContainer=!1}Pd(this._lView[E],this._lView)}onDestroy(n){Is(this._lView,n)}markForCheck(){Gd(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[O]&=-129}reattach(){Es(this._lView),this._lView[O]|=128}detectChanges(){this._lView[O]|=1024,Lh(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new D(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Ki(this._lView),t=this._lView[yn];t!==null&&!n&&Fd(t,this._lView),yh(this._lView[E],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new D(902,!1);this._appRef=n;let t=Ki(this._lView),i=this._lView[yn];i!==null&&!t&&Hh(i,this._lView),Es(this._lView)}};var Xt=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=Fw;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let o=Wd(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Mn(o)}}return e})();function Fw(){return ua(Ne(),q())}function ua(e,n){return e.type&4?new Xt(n,e,sr(e,n)):null}function cr(e,n,t,i,r){let o=e.data[n];if(o===null)o=Pw(e,n,t,i,r),Lm()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=i,o.attrs=r;let s=Nm();o.injectorIndex=s===null?-1:s.injectorIndex}return Qi(o,!0),o}function Pw(e,n,t,i,r){let o=wl(),s=xl(),a=s?o:o&&o.parent,c=e.data[n]=Vw(e,a,t,n,i,r);return Lw(e,c,o,s),c}function Lw(e,n,t,i){e.firstChild===null&&(e.firstChild=n),t!==null&&(i?t.child==null&&n.parent!==null&&(t.child=n):t.next===null&&(t.next=n,n.prev=t))}function Vw(e,n,t,i,r,o){let s=n?n.injectorIndex:-1,a=0;return _l()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Sl(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var Bw=()=>null,jw=()=>null;function ld(e,n){return Bw(e,n)}function zw(e,n,t){return jw(e,n,t)}var Uh=class{},Se=class{},Ke=(()=>{class e{destroyNode=null;static __NG_ELEMENT_ID__=()=>Hw()}return e})();function Hw(){let e=q(),n=Ne(),t=st(n.index,e);return(Zt(t)?t:e)[ge]}var $h=(()=>{class e{static \u0275prov=X({token:e,providedIn:"root",factory:()=>null})}return e})();function Wh(e){return e.debugInfo?.className||e.type.name||null}var Us={},Qs=class{injector;parentInjector;constructor(n,t){this.injector=n,this.parentInjector=t}get(n,t,i){let r=this.injector.get(n,Us,i);return r!==Us||t===Us?r:this.parentInjector.get(n,t,i)}};function pi(e,n,t){if(t===xt)return!1;let i=e[n];return Object.is(i,t)?!1:(e[n]=t,!0)}function Ll(e,n,t){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&Yb(r,o);let s=Yt(e)?st(e.index,n):n;Gd(s,5);let a=n[Oe],c=yp(n,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=yp(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function yp(e,n,t,i){let r=j(null);try{return oe(ne.OutputStart,n,t),t(i)!==!1}catch(o){return gw(e,o),!1}finally{oe(ne.OutputEnd,n,t),j(r)}}function Uw(e,n,t,i,r,o,s,a){let c=Br(e),l=!1,d=null;if(!i&&c&&(d=Ww(n,t,o,e.index)),d!==null){let h=d.__ngLastListenerFn__||d;h.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let h=gt(e,t),v=i?i(h):h;Qb(t,v,o,a),i||(a.__ngNativeEl__=h);let g=r.listen(v,o,a);if(!$w(o)){let _=i?b=>i(ot(b[e.index])):e.index;Gh(_,n,t,o,a,g,!1)}}return l}function $w(e){return e.startsWith("animation")||e.startsWith("transition")}function Ww(e,n,t,i){let r=e.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===t&&r[o+1]===i){let a=n[Zi],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function Gh(e,n,t,i,r,o,s){let a=n.firstCreatePass?hl(n):null,c=pl(t),l=c.length;c.push(r,o),a&&a.push(i,e,l,(l+1)*(s?-1:1))}function bp(e,n,t,i,r,o){let s=n[t],a=n[E],l=a.data[t].outputs[i],h=s[l].subscribe(o);Gh(e.index,a,n,r,o,h,!0)}var dd=Symbol("BINDING");var hi=new y("");function Xs(e,n,t){let i=t?e.styles:null,r=t?e.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=hs(r,a);else if(o==2){let c=a,l=n[++s];i=hs(i,c+": "+l+";")}}t?e.styles=i:e.stylesWithoutHost=i,t?e.classes=r:e.classesWithoutHost=r}function lr(e,n=0){let t=q();if(t===null)return T(e,n);let i=Ne();return $p(i,t,Ae(e),n)}function qh(e,n,t,i,r){let o=i===null?null:{"":-1},s=r(e,t);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}Zw(e,n,t,a,o,c,l)}o!==null&&i!==null&&Gw(t,i,o)}function Gw(e,n,t){let i=e.localNames=[];for(let r=0;r<n.length;r+=2){let o=t[n[r+1]];if(o==null)throw new D(-301,!1);i.push(n[r],o)}}function qw(e,n,t){n.componentOffset=t,(e.components??=[]).push(n.index)}function Zw(e,n,t,i,r,o,s){let a=i.length,c=null;for(let v=0;v<a;v++){let g=i[v];c===null&&At(g)&&(c=g,qw(e,t,v)),Gl(qs(t,n),e,g.type)}ex(t,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let v=0;v<a;v++){let g=i[v];g.providersResolver&&g.providersResolver(g)}let l=!1,d=!1,h=Eh(e,n,a,null);a>0&&(t.directiveToIndex=new Map);for(let v=0;v<a;v++){let g=i[v];if(t.mergedAttrs=nr(t.mergedAttrs,g.hostAttrs),Kw(e,t,n,h,g),Jw(h,g,r),s!==null&&s.has(g)){let[b,I]=s.get(g);t.directiveToIndex.set(g.type,[h,b+t.directiveStart,I+t.directiveStart])}else(o===null||!o.has(g))&&t.directiveToIndex.set(g.type,h);g.contentQueries!==null&&(t.flags|=4),(g.hostBindings!==null||g.hostAttrs!==null||g.hostVars!==0)&&(t.flags|=64);let _=g.type.prototype;!l&&(_.ngOnChanges||_.ngOnInit||_.ngDoCheck)&&((e.preOrderHooks??=[]).push(t.index),l=!0),!d&&(_.ngOnChanges||_.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(t.index),d=!0),h++}Yw(e,t,o)}function Yw(e,n,t){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=e.data[i];if(t===null||!t.has(r))wp(0,n,r,i),wp(1,n,r,i),Cp(n,i,!1);else{let o=t.get(r);xp(0,n,o,i),xp(1,n,o,i),Cp(n,i,!0)}}}function wp(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;e===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),Zh(n,o)}}function xp(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;e===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),Zh(n,s)}}function Zh(e,n){n==="class"?e.flags|=8:n==="style"&&(e.flags|=16)}function Cp(e,n,t){let{attrs:i,inputs:r,hostDirectiveInputs:o}=e;if(i===null||!t&&r===null||t&&o===null||Ad(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function Kw(e,n,t,i,r){e.data[i]=r;let o=r.factory||(r.factory=Zn(r.type,!0)),s=new li(o,At(r),lr,null);e.blueprint[i]=s,t[i]=s,Qw(e,n,i,Eh(e,t,r.hostVars,xt),r)}function Qw(e,n,t,i,r){let o=r.hostBindings;if(o){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~n.index;Xw(s)!=a&&s.push(a),s.push(t,i,o)}}function Xw(e){let n=e.length;for(;n>0;){let t=e[--n];if(typeof t=="number"&&t<0)return t}return 0}function Jw(e,n,t){if(t){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)t[n.exportAs[i]]=e;At(n)&&(t[""]=e)}}function ex(e,n,t){e.flags|=1,e.directiveStart=n,e.directiveEnd=n+t,e.providerIndexes=n}function Yh(e,n,t,i,r,o,s,a){let c=n[E],l=c.consts,d=Ot(l,s),h=cr(c,e,t,i,d);return o&&qh(c,n,h,Ot(l,a),r),h.mergedAttrs=nr(h.mergedAttrs,h.attrs),h.attrs!==null&&Xs(h,h.attrs,!1),h.mergedAttrs!==null&&Xs(h,h.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,h),h}function Kh(e,n){Fp(e,n),ll(n)&&e.queries.elementEnd(n)}function tx(e,n,t,i,r,o){let s=n.consts,a=Ot(s,r),c=cr(n,e,t,i,a);if(c.mergedAttrs=nr(c.mergedAttrs,c.attrs),o!=null){let l=Ot(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Xs(c,c.attrs,!1),c.mergedAttrs!==null&&Xs(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var Qh=typeof ShadowRoot<"u",nx=typeof Document<"u";function ix(e){return Object.keys(e).map(n=>{let[t,i,r]=e[n],o={propName:t,templateName:n,isSignal:(i&la.SignalBased)!==0};return r&&(o.transform=r),o})}function rx(e){return Object.keys(e).map(n=>({propName:e[n],templateName:n}))}function ox(e,n,t){let i=n instanceof be?n:n?.injector;return i&&e.getStandaloneInjector!==null&&(i=e.getStandaloneInjector(i)||i),i?new Qs(t,i):t}function sx(e){let n=e.get(Se,null);if(n===null)throw new D(407,!1);let t=e.get($h,null),i=e.get(St,null),r=e.get(Vt,null,{optional:!0});return{rendererFactory:n,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function ax(e,n){let t=Xh(e);return uh(n,t,t==="svg"?dl:t==="math"?Em:null)}function Xh(e){return(e.selectors[0][0]||"div").toLowerCase()}var rr=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=ix(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=rx(this.componentDef.outputs),this.cachedOutputs}constructor(n,t){this.componentDef=n,this.ngModule=t,this.componentType=n.type,this.selector=A0(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!t}create(n,t,i,r,o,s){oe(ne.DynamicComponentStart);let a=j(null);try{let c=this.componentDef,l=ox(c,r||this.ngModule,n),d=sx(l),h=d.tracingService;return h&&h.componentCreate?h.componentCreate(Wh(c),()=>this.createComponentRef(d,l,t,i,o,s)):this.createComponentRef(d,l,t,i,o,s)}finally{j(a)}}createComponentRef(n,t,i,r,o,s){let a=this.componentDef,c=cx(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?rw(l,r,a.encapsulation,t):ax(a,l),h=t.get(hi,null),v=lx(d,()=>t.get(P,null)??Jp());h&&h.addHost(v);let g=s?.some(Dp)||o?.some(I=>typeof I!="function"&&I.bindings.some(Dp)),_=jd(null,c,null,512|Dh(a),null,null,n,l,t,null,nh(d,t,!0));h&&Qh&&v instanceof ShadowRoot&&Is(_,()=>{h.removeHost(v)}),_[xe]=d,ks(_);let b=null;try{let I=Yh(xe,_,2,"#host",()=>c.directiveRegistry,!0,0);mh(l,d,I),ir(d,_),Hd(c,_,I),rh(c,I,_),Kh(c,I),i!==void 0&&ux(I,this.ngContentSelectors,i),b=st(I.index,_),_[Oe]=b[Oe],$d(c,_,null)}catch(I){throw b!==null&&Zl(b),Zl(_),I}finally{oe(ne.DynamicComponentEnd),Ts()}return new Js(this.componentType,_,!!g)}};function cx(e,n,t,i){let r=e?["ng-version","22.0.2"]:O0(n.selectors[0]),o=null,s=null,a=0;if(t)for(let d of t)a+=d[dd].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let h=i[d];if(typeof h!="function")for(let v of h.bindings){a+=v[dd].requiredVars;let g=d+1;v.create&&(v.targetIdx=g,(o??=[]).push(v)),v.update&&(v.targetIdx=g,(s??=[]).push(v))}}let c=[n];if(i)for(let d of i){let h=typeof d=="function"?d:d.type,v=Qc(h);c.push(v)}return Bd(0,null,dx(o,s),1,a,c,null,null,null,[r],null)}function lx(e,n){let t=e.getRootNode?.();return nx&&t instanceof Document?t.head:t&&Qh&&t instanceof ShadowRoot?t:n().head}function dx(e,n){return!e&&!n?null:t=>{if(t&1&&e)for(let i of e)i.create();if(t&2&&n)for(let i of n)i.update()}}function Dp(e){let n=e[dd].kind;return n==="input"||n==="twoWay"}var Js=class extends Uh{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Cs(t[E],xe),this.location=sr(this._tNode,t),this.instance=st(this._tNode.index,t)[Oe],this.hostView=this.changeDetectorRef=new Mn(t,void 0),this.componentType=n}setInput(n,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),t))return;let r=this._rootLView,o=Ud(i,r[E],r,n,t);this.previousInputValues.set(n,t);let s=st(i.index,r);Gd(s,1)}get injector(){return new In(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function ux(e,n,t){let i=e.projection=[];for(let r=0;r<n.length;r++){let o=t[r];i.push(o!=null&&o.length?Array.from(o):null)}}var tn=(()=>{class e{static __NG_ELEMENT_ID__=fx}return e})();function fx(){let e=Ne();return Jh(e,q())}var ud=class e extends tn{_lContainer;_hostTNode;_hostLView;constructor(n,t,i){super(),this._lContainer=n,this._hostTNode=t,this._hostLView=i}get element(){return sr(this._hostTNode,this._hostLView)}get injector(){return new In(this._hostTNode,this._hostLView)}get parentInjector(){let n=wd(this._hostTNode,this._hostLView);if(Vp(n)){let t=Gs(n,this._hostLView),i=Ws(n),r=t[E].data[i+8];return new In(r,t)}else return new In(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let t=Ep(this._lContainer);return t!==null&&t[n]||null}get length(){return this._lContainer.length-Re}createEmbeddedView(n,t,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=ld(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,r,Ys(this._hostTNode,s)),a}createComponent(n,t,i,r,o,s,a){let c,l=t||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new rr(gn(n)),h=i||this.parentInjector;if(!o&&d.ngModule==null){let G=this.parentInjector.get(be,null);G&&(o=G)}let v=gn(d.componentType??{}),g=ld(this._lContainer,v?.id??null),_=g?.firstChild??null,b=d.create(h,r,_,o,s,a);return this.insertImpl(b.hostView,c,Ys(this._hostTNode,g)),b}insert(n,t){return this.insertImpl(n,t,!0)}insertImpl(n,t,i){let r=n._lView;if(Sm(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[Me],l=new e(c,c[Be],c[Me]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(t),s=this._lContainer;return qd(s,r,o,i),n.attachToViewContainerRef(),nl(Vl(s),o,n),n}move(n,t){return this.insert(n,t)}indexOf(n){let t=Ep(this._lContainer);return t!==null?t.indexOf(n):-1}remove(n){let t=this._adjustIndex(n,-1),i=Ks(this._lContainer,t);i&&(Fr(Vl(this._lContainer),t),Pd(i[E],i))}detach(n){let t=this._adjustIndex(n,-1),i=Ks(this._lContainer,t);return i&&Fr(Vl(this._lContainer),t)!=null?new Mn(i):null}_adjustIndex(n,t=0){return n??this.length+t}};function Ep(e){return e[Vr]}function Vl(e){return e[Vr]||(e[Vr]=[])}function Jh(e,n){let t,i=n[e.index];return rt(i)?t=i:(t=zh(i,n,null,e),n[e.index]=t,zd(n,t)),px(t,n,e,i),new ud(t,e,n)}function mx(e,n){let t=e[ge],i=t.createComment(""),r=gt(n,e),o=t.parentNode(r);return Zs(t,o,i,t.nextSibling(r),!1),i}var px=vx,hx=()=>!1;function gx(e,n,t){return hx(e,n,t)}function vx(e,n,t,i){if(e[wn])return;let r;t.type&8?r=ot(i):r=mx(n,t),e[wn]=r}var fd=class e{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},md=class e{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let t=n.queries;if(t!==null){let i=n.contentQueries!==null?n.contentQueries[0]:t.length,r=[];for(let o=0;o<i;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new e(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let t=0;t<this.queries.length;t++)Yd(n,t).matches!==null&&this.queries[t].setDirty()}},ea=class{flags;read;predicate;constructor(n,t,i=null){this.flags=t,this.read=i,typeof n=="string"?this.predicate=xx(n):this.predicate=n}},pd=class e{queries;constructor(n=[]){this.queries=n}elementStart(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,t)}elementEnd(n){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(n)}embeddedTView(n){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,t!==null?t.push(o):t=[o])}return t!==null?new e(t):null}template(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,t)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},hd=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,t=-1){this.metadata=n,this._declarationNodeIndex=t}elementStart(n,t){this.isApplyingToNode(t)&&this.matchTNode(n,t)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,t){this.elementStart(n,t)}embeddedTView(n,t){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,t),new e(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,t,_x(t,o)),this.matchTNodeWithReadOption(n,t,Hs(t,n,o,!1,!1))}else i===Xt?t.type&4&&this.matchTNodeWithReadOption(n,t,-1):this.matchTNodeWithReadOption(n,t,Hs(t,n,i,!1,!1))}matchTNodeWithReadOption(n,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===B||r===tn||r===Xt&&t.type&4)this.addMatch(t.index,-2);else{let o=Hs(t,n,r,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,i)}}addMatch(n,t){this.matches===null?this.matches=[n,t]:this.matches.push(n,t)}};function _x(e,n){let t=e.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===n)return t[i+1]}return null}function yx(e,n){return e.type&11?sr(e,n):e.type&4?ua(e,n):null}function bx(e,n,t,i){return t===-1?yx(n,e):t===-2?wx(e,n,i):qr(e,e[E],t,n)}function wx(e,n,t){if(t===B)return sr(n,e);if(t===Xt)return ua(n,e);if(t===tn)return Jh(n,e)}function eg(e,n,t,i){let r=n[Tt].queries[i];if(r.matches===null){let o=e.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(bx(n,d,s[c+1],t.metadata.read))}}r.matches=a}return r.matches}function gd(e,n,t,i){let r=e.queries.getByIndex(t),o=r.matches;if(o!==null){let s=eg(e,n,r,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let h=Re;h<d.length;h++){let v=d[h];v[yn]===v[Me]&&gd(v[E],v,l,i)}if(d[ti]!==null){let h=d[ti];for(let v=0;v<h.length;v++){let g=h[v];gd(g[E],g,l,i)}}}}}return i}function Zd(e,n){return e[Tt].queries[n].queryList}function tg(e,n,t){let i=new bt((t&4)===4);return Am(e,n,i,i.destroy),(n[Tt]??=new md).queries.push(new fd(i))-1}function ng(e,n,t){let i=De();return i.firstCreatePass&&(rg(i,new ea(e,n,t),-1),(n&2)===2&&(i.staticViewQueries=!0)),tg(i,q(),n)}function ig(e,n,t,i){let r=De();if(r.firstCreatePass){let o=Ne();rg(r,new ea(n,t,i),o.index),Cx(r,e),(t&2)===2&&(r.staticContentQueries=!0)}return tg(r,q(),t)}function xx(e){return e.split(",").map(n=>n.trim())}function rg(e,n,t){e.queries===null&&(e.queries=new pd),e.queries.track(new hd(n,t))}function Cx(e,n){let t=e.contentQueries||(e.contentQueries=[]),i=t.length?t[t.length-1]:-1;n!==i&&t.push(e.queries.length-1,n)}function Yd(e,n){return e.queries.getByIndex(n)}function og(e,n){let t=e[E],i=Yd(t,n);return i.crossesNgTemplate?gd(t,e,n,[]):eg(t,e,i,n)}function sg(e,n,t){let i,r=wr(()=>{i._dirtyCounter();let o=Dx(i,e);if(n&&o===void 0)throw new D(-951,!1);return o});return i=r[Ie],i._dirtyCounter=Ee(0),i._flatValue=void 0,r}function Kd(e){return sg(!0,!1,e)}function Qd(e){return sg(!0,!0,e)}function ag(e,n){let t=e[Ie];t._lView=q(),t._queryIndex=n,t._queryList=Zd(t._lView,n),t._queryList.onDirty(()=>t._dirtyCounter.update(i=>i+1))}function Dx(e,n){let t=e._lView,i=e._queryIndex;if(t===void 0||i===void 0||t[O]&4)return n?void 0:Ve;let r=Zd(t,i),o=og(t,i);return r.reset(o,qp),n?r.first:r._changesDetected||e._flatValue===void 0?e._flatValue=r.toArray():e._flatValue}function eo(e){return!!e&&typeof e.then=="function"}function Xd(e){return!!e&&typeof e.subscribe=="function"}var ui=class{};var Kr=class extends ui{injector;instance=null;constructor(n){super();let t=new Kn([...n.providers,{provide:ui,useValue:this}],n.parent||Wi(),n.debugName,new Set(["environment"]));this.injector=t,n.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function cg(e,n,t=null){return new Kr({providers:e,parent:n,debugName:t,runEnvironmentInitializers:!0}).injector}var Ex=(()=>{class e{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=rl(!1,t.type),r=i.length>0?cg([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=X({token:e,providedIn:"environment",factory:()=>new e(T(be))})}return e})();function R(e){return Xr(()=>{let n=lg(e),t=ye(V({},n),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection!==Cd.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&e.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(Ex).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||wt.Emulated,styles:e.styles||Ve,_:null,schemas:e.schemas||null,tView:null,id:""});n.standalone&&ar("NgStandalone"),dg(t);let i=e.dependencies;return t.directiveDefs=Ip(i,Ix),t.pipeDefs=Ip(i,dm),t.id=kx(t),t})}function Ix(e){return gn(e)||Qc(e)}function A(e){return Xr(()=>({type:e.type,bootstrap:e.bootstrap||Ve,declarations:e.declarations||Ve,imports:e.imports||Ve,exports:e.exports||Ve,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function Mx(e,n){if(e==null)return vn;let t={};for(let i in e)if(e.hasOwnProperty(i)){let r=e[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=la.None,c=null),t[o]=[i,a,c],n[o]=s}return t}function Sx(e){if(e==null)return vn;let n={};for(let t in e)e.hasOwnProperty(t)&&(n[e[t]]=t);return n}function $(e){return Xr(()=>{let n=lg(e);return dg(n),n})}function lg(e){let n={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:n,inputConfig:e.inputs||vn,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||Ve,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:Mx(e.inputs,n),outputs:Sx(e.outputs),debugInfo:null}}function dg(e){e.features?.forEach(n=>n(e))}function Ip(e,n){return e?()=>{let t=typeof e=="function"?e():e,i=[];for(let r of t){let o=n(r);o!==null&&i.push(o)}return i}:null}function kx(e){let n=0,t=typeof e.consts=="function"?"":e.consts,i=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,t,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var ug=new y("");var Jd=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=m(ug,{optional:!0})??[];injector=m(ee);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let o=Gi(this.injector,r);if(eo(o))t.push(o);else if(Xd(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();function Tx(e){return Object.getPrototypeOf(e.prototype).constructor}function _e(e){let n=Tx(e.type),t=!0,i=[e];for(;n;){let r;if(At(e))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new D(903,!1);r=n.\u0275dir}if(r){if(t){i.push(r);let s=e;s.inputs=Bl(e.inputs),s.declaredInputs=Bl(e.declaredInputs),s.outputs=Bl(e.outputs);let a=r.hostBindings;a&&Fx(e,a);let c=r.viewQuery,l=r.contentQueries;if(c&&Rx(e,c),l&&Nx(e,l),Ax(e,r),lm(e.outputs,r.outputs),At(r)&&r.data.animation){let d=e.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(e),a===_e&&(t=!1)}}n=Object.getPrototypeOf(n)}Ox(i)}function Ax(e,n){for(let t in n.inputs){if(!n.inputs.hasOwnProperty(t)||e.inputs.hasOwnProperty(t))continue;let i=n.inputs[t];i!==void 0&&(e.inputs[t]=i,e.declaredInputs[t]=n.declaredInputs[t])}}function Ox(e){let n=0,t=null;for(let i=e.length-1;i>=0;i--){let r=e[i];r.hostVars=n+=r.hostVars,r.hostAttrs=nr(r.hostAttrs,t=nr(t,r.hostAttrs))}}function Bl(e){return e===vn?{}:e===Ve?[]:e}function Rx(e,n){let t=e.viewQuery;t?e.viewQuery=(i,r)=>{n(i,r),t(i,r)}:e.viewQuery=n}function Nx(e,n){let t=e.contentQueries;t?e.contentQueries=(i,r,o)=>{n(i,r,o),t(i,r,o)}:e.contentQueries=n}function Fx(e,n){let t=e.hostBindings;t?e.hostBindings=(i,r)=>{n(i,r),t(i,r)}:e.hostBindings=n}function fg(e,n,t,i,r,o,s,a){if(t.firstCreatePass){e.mergedAttrs=nr(e.mergedAttrs,e.attrs);let d=e.tView=Bd(2,e,r,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,e),d.queries=t.queries.embeddedTView(e))}a&&(e.flags|=a),Qi(e,!1);let c=Lx(t,n,e,i);Os()&&Ld(t,n,c,e),ir(c,n);let l=zh(c,n,c,e);n[i+xe]=l,zd(n,l),gx(l,e,n)}function Px(e,n,t,i,r,o,s,a,c,l,d){let h=t+xe,v;return n.firstCreatePass?(v=cr(n,h,4,s||null,a||null),vl()&&qh(n,e,v,Ot(n.consts,l),kh),Fp(n,v)):v=n.data[h],fg(v,e,n,t,i,r,o,c),Br(v)&&Hd(n,e,v),l!=null&&da(e,v,d),v}function eu(e,n,t,i,r,o,s,a,c,l,d){let h=t+xe,v;if(n.firstCreatePass){if(v=cr(n,h,4,s||null,a||null),l!=null){let g=Ot(n.consts,l);v.localNames=[];for(let _=0;_<g.length;_+=2)v.localNames.push(g[_],-1)}}else v=n.data[h];return fg(v,e,n,t,i,r,o,c),l!=null&&da(e,v,d),v}function nn(e,n,t,i,r,o,s,a){let c=q(),l=De(),d=Ot(l.consts,o);return Px(c,l,e,n,t,i,r,d,void 0,s,a),nn}var Lx=Vx;function Vx(e,n,t,i){return Rs(!0),n[ge].createComment("")}var tu=new y("");var nu=new y("");function mg(){hc(()=>{let e="";throw new D(600,e)})}var Bx=10;var Ct=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=m(Kt);afterRenderManager=m(sa);zonelessEnabled=m(Ur);rootEffectScheduler=m(Fs);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new w;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=m(ri);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ae(t=>!t))}constructor(){m(Vt,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=m(be);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=ee.NULL){return this._injector.get(C).run(()=>{if(oe(ne.BootstrapComponentStart),!this._injector.get(Jd).done){let G="";throw new D(405,G)}let a=gn(t),c=this._injector.get(ui),l=new rr(a,c);this.componentTypes.push(t);let{hostElement:d,directives:h,bindings:v}=jx(i),g=d||l.selector,_=l.create(r,[],g,c.injector,h,v),b=_.location.nativeElement,I=_.injector.get(tu,null);return I?.registerApplication(b),_.onDestroy(()=>{this.detachView(_.hostView),Gr(this.components,_),I?.unregisterApplication(b)}),this._loadComponent(_),oe(ne.BootstrapComponentEnd,_),_})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){oe(ne.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(oa.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw oe(ne.ChangeDetectionEnd),new D(101,!1);let t=j(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,j(t),this.afterTick.next(),oe(ne.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Se,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<Bx;){oe(ne.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{oe(ne.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!jr(r))continue;let o=i&&!this.zonelessEnabled?0:1;Lh(r,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>jr(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;Gr(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(nu,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>Gr(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new D(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();function jx(e){return e===void 0||typeof e=="string"||e instanceof Element?{hostElement:e}:e}function Gr(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function ie(e,n,t,i){let r=q(),o=Xi();if(pi(r,o,n)){let s=De(),a=As();mw(a,r,e,n,t,i)}return ie}function Z(e,n,t,i,r,o,s,a){ar("NgControlFlow");let c=q(),l=De(),d=Ot(l.consts,o);return eu(c,l,e,n,t,i,r,d,256,s,a),iu}function iu(e,n,t,i,r,o,s,a){ar("NgControlFlow");let c=q(),l=De(),d=Ot(l.consts,o);return eu(c,l,e,n,t,i,r,d,512,s,a),iu}function Y(e,n){ar("NgControlFlow");let t=q(),i=Xi(),r=t[i]!==xt?t[i]:-1,o=r!==-1?Mp(t,xe+r):void 0,s=0;if(pi(t,i,e)){let a=j(null);try{if(o!==void 0&&Rw(o,s),e!==-1){let c=xe+e,l=Mp(t,c),d=zx(t[E],c),h=zw(l,d,t),v=Wd(t,d,n,{dehydratedView:h});qd(l,v,s,Ys(d,h))}}finally{j(a)}}else if(o!==void 0){let a=Ow(o,s);a!==void 0&&(a[Oe]=n)}}function Mp(e,n){return e[n]}function zx(e,n){return Cs(e,n)}function Fe(e,n,t){let i=q(),r=Xi();if(pi(i,r,n)){let o=De(),s=As();cw(s,i,e,n,i[ge],t)}return Fe}function vd(e,n,t,i,r){Ud(n,e,t,r?"class":"style",i)}function u(e,n,t,i){let r=q(),o=r[E],s=e+xe,a=o.firstCreatePass?Yh(s,r,2,n,kh,vl(),t,i):o.data[s];if(Yt(a)){let c=r[kt].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(Wh(l),()=>(Sp(e,n,r,a,i),u))}}return Sp(e,n,r,a,i),u}function Sp(e,n,t,i,r){if(Th(i,t,e,n,pg),Br(i)){let o=t[E];Hd(o,t,i),rh(o,i,t)}r!=null&&da(t,i)}function f(){let e=De(),n=Ne(),t=Ah(n);return e.firstCreatePass&&Kh(e,t),yl(t)&&bl(),gl(),t.classesWithoutHost!=null&&Ab(t)&&vd(e,t,q(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&Ob(t)&&vd(e,t,q(),t.stylesWithoutHost,!1),f}function W(e,n,t,i){return u(e,n,t,i),f(),W}function je(e,n,t,i){let r=q(),o=r[E],s=e+xe,a=o.firstCreatePass?tx(s,o,2,n,t,i):o.data[s];return Th(a,r,e,n,pg),i!=null&&da(r,a),je}function ze(){let e=Ne(),n=Ah(e);return yl(n)&&bl(),gl(),ze}function lt(e,n,t,i){return je(e,n,t,i),ze(),lt}var pg=(e,n,t,i,r)=>(Rs(!0),uh(n[ge],i,Sl()));function Sn(){return q()}function Dt(e,n,t){let i=q(),r=Xi();if(pi(i,r,n)){let o=De(),s=As();Sh(s,i,e,n,i[ge],t)}return Dt}var to="en-US";var Hx=to;function hg(e){typeof e=="string"&&(Hx=e.toLowerCase().replace(/_/g,"-"))}function ue(e,n,t){let i=q(),r=De(),o=Ne();return Ux(r,i,i[ge],o,e,n,t),ue}function Ux(e,n,t,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Ll(i,n,o),Uw(i,e,n,s,t,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let h=0;h<d.length;h+=2){let v=d[h],g=d[h+1];c??=Ll(i,n,o),bp(i,n,v,g,r,c)}if(l&&l.length)for(let h of l)c??=Ll(i,n,o),bp(i,n,h,r,r,c)}}function he(e=1){return $m(e)}function $x(e,n){let t=null,i=I0(e);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){t=r;continue}if(i===null?hh(e,o,!0):k0(i,o))return r}return t}function re(e){let n=q()[Ye][Be];if(!n.projection){let t=e?e.length:1,i=n.projection=gm(t,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=e?$x(o,e):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function x(e,n=0,t,i,r,o){let s=q(),a=De(),c=i?e+1:null;c!==null&&eu(s,a,c,i,r,o,null,t);let l=cr(a,xe+e,16,null,t||null);l.projection===null&&(l.projection=n),Cl();let h=!s[qi]||_l();s[Ye][Be].projection[l.projection]===null&&c!==null?Wx(s,a,c):h&&!na(l)&&X0(a,s,l)}function Wx(e,n,t){let i=xe+t,r=n.data[i],o=e[i],s=ld(o,r.tView.ssrId),a=Wd(e,r,void 0,{dehydratedView:s});qd(o,a,0,Ys(r,s))}function He(e,n,t,i){return ig(e,n,t,i),He}function Pe(e,n,t){return ng(e,n,t),Pe}function S(e){let n=q(),t=De(),i=Ss();zr(i+1);let r=Yd(t,i);if(e.dirty&&Mm(n)===((r.metadata.flags&2)===2)){if(r.matches===null)e.reset([]);else{let o=og(n,i);e.reset(o,qp),e.notifyOnChanges()}return!0}return!1}function k(){return Zd(q(),Ss())}function fa(e,n,t,i,r){return ag(n,ig(e,t,i,r)),fa}function ma(e,n,t,i){return ag(e,ng(n,t,i)),ma}function pa(e=1){zr(Ss()+e)}function Bt(e){let n=Fm();return Im(n,xe+e)}function Bs(e,n){return e<<17|n<<2}function fi(e){return e>>17&32767}function Gx(e){return(e&2)==2}function qx(e,n){return e&131071|n<<17}function _d(e){return e|2}function or(e){return(e&131068)>>2}function jl(e,n){return e&-131069|n<<2}function Zx(e){return(e&1)===1}function yd(e){return e|1}function Yx(e,n,t,i,r,o){let s=o?n.classBindings:n.styleBindings,a=fi(s),c=or(s);e[i]=t;let l=!1,d;if(Array.isArray(t)){let h=t;d=h[1],(d===null||Ui(h,d)>0)&&(l=!0)}else d=t;if(r)if(c!==0){let v=fi(e[a+1]);e[i+1]=Bs(v,a),v!==0&&(e[v+1]=jl(e[v+1],i)),e[a+1]=qx(e[a+1],i)}else e[i+1]=Bs(a,0),a!==0&&(e[a+1]=jl(e[a+1],i)),a=i;else e[i+1]=Bs(c,0),a===0?a=i:e[c+1]=jl(e[c+1],i),c=i;l&&(e[i+1]=_d(e[i+1])),kp(e,d,i,!0),kp(e,d,i,!1),Kx(n,d,e,i,o),s=Bs(a,c),o?n.classBindings=s:n.styleBindings=s}function Kx(e,n,t,i,r){let o=r?e.residualClasses:e.residualStyles;o!=null&&typeof n=="string"&&Ui(o,n)>=0&&(t[i+1]=yd(t[i+1]))}function kp(e,n,t,i){let r=e[t+1],o=n===null,s=i?fi(r):or(r),a=!1;for(;s!==0&&(a===!1||o);){let c=e[s],l=e[s+1];Qx(c,n)&&(a=!0,e[s+1]=i?yd(l):_d(l)),s=i?fi(l):or(l)}a&&(e[t+1]=i?_d(r):yd(r))}function Qx(e,n){return e===null||n==null||(Array.isArray(e)?e[1]:e)===n?!0:Array.isArray(e)&&typeof n=="string"?Ui(e,n)>=0:!1}var yt={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function Xx(e){return e.substring(yt.key,yt.keyEnd)}function Jx(e){return eC(e),gg(e,vg(e,0,yt.textEnd))}function gg(e,n){let t=yt.textEnd;return t===n?-1:(n=yt.keyEnd=tC(e,yt.key=n,t),vg(e,n,t))}function eC(e){yt.key=0,yt.keyEnd=0,yt.value=0,yt.valueEnd=0,yt.textEnd=e.length}function vg(e,n,t){for(;n<t&&e.charCodeAt(n)<=32;)n++;return n}function tC(e,n,t){for(;n<t&&e.charCodeAt(n)>32;)n++;return n}function kn(e,n,t){return _g(e,n,t,!1),kn}function N(e,n){return _g(e,n,null,!0),N}function jt(e){iC(lC,nC,e,!0)}function nC(e,n){for(let t=Jx(n);t>=0;t=gg(n,t))bs(e,Xx(n),!0)}function _g(e,n,t,i){let r=q(),o=De(),s=El(2);if(o.firstUpdatePass&&bg(o,e,s,i),n!==xt&&pi(r,s,n)){let a=o.data[xn()];wg(o,a,r,r[ge],e,r[s+1]=uC(n,t),i,s)}}function iC(e,n,t,i){let r=De(),o=El(2);r.firstUpdatePass&&bg(r,null,o,i);let s=q();if(t!==xt&&pi(s,o,t)){let a=r.data[xn()];if(xg(a,i)&&!yg(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=hs(c,t||"")),vd(r,a,s,t,i)}else dC(r,a,s,s[ge],s[o+1],s[o+1]=cC(e,n,t),i,o)}}function yg(e,n){return n>=e.expandoStartIndex}function bg(e,n,t,i){let r=e.data;if(r[t+1]===null){let o=r[xn()],s=yg(e,t);xg(o,i)&&n===null&&!s&&(n=!1),n=rC(r,o,n,i),Yx(r,o,n,t,s,i)}}function rC(e,n,t,i){let r=jm(e),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(t=zl(null,e,n,t,i),t=Qr(t,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||e[s]!==r)if(t=zl(r,e,n,t,i),o===null){let c=oC(e,n,i);c!==void 0&&Array.isArray(c)&&(c=zl(null,e,n,c[1],i),c=Qr(c,n.attrs,i),sC(e,n,i,c))}else o=aC(e,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),t}function oC(e,n,t){let i=t?n.classBindings:n.styleBindings;if(or(i)!==0)return e[fi(i)]}function sC(e,n,t,i){let r=t?n.classBindings:n.styleBindings;e[fi(r)]=i}function aC(e,n,t){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=e[o].hostAttrs;i=Qr(i,s,t)}return Qr(i,n.attrs,t)}function zl(e,n,t,i,r){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=n[a],i=Qr(i,o.hostAttrs,r),o!==e);)a++;return e!==null&&(t.directiveStylingLast=a),i}function Qr(e,n,t){let i=t?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),bs(e,s,t?!0:n[++o]))}return e===void 0?null:e}function cC(e,n,t){if(t==null||t==="")return Ve;let i=[],r=Jt(t);if(Array.isArray(r))for(let o=0;o<r.length;o++)e(i,r[o],!0);else if(r instanceof Set)for(let o of r)e(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&e(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function lC(e,n,t){let i=String(n);i!==""&&!i.includes(" ")&&bs(e,i,t)}function dC(e,n,t,i,r,o,s,a){r===xt&&(r=Ve);let c=0,l=0,d=0<r.length?r[0]:null,h=0<o.length?o[0]:null;for(;d!==null||h!==null;){let v=c<r.length?r[c+1]:void 0,g=l<o.length?o[l+1]:void 0,_=null,b;d===h?(c+=2,l+=2,v!==g&&(_=h,b=g)):h===null||d!==null&&d<h?(c+=2,_=d):(l+=2,_=h,b=g),_!==null&&wg(e,n,t,i,_,b,s,a),d=c<r.length?r[c]:null,h=l<o.length?o[l]:null}}function wg(e,n,t,i,r,o,s,a){if(!(n.type&3))return;let c=e.data,l=c[a+1],d=Zx(l)?Tp(c,n,t,r,or(l),s):void 0;if(!ta(d)){ta(o)||Gx(l)&&(o=Tp(c,null,t,r,a,s));let h=ul(xn(),t);ew(i,s,h,r,o)}}function Tp(e,n,t,i,r,o){let s=n===null,a;for(;r>0;){let c=e[r],l=Array.isArray(c),d=l?c[1]:c,h=d===null,v=t[r+1];v===xt&&(v=h?Ve:void 0);let g=h?ws(v,i):d===i?v:void 0;if(l&&!ta(g)&&(g=ws(c,i)),ta(g)&&(a=g,s))return a;let _=e[r+1];r=s?fi(_):or(_)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=ws(c,i))}return a}function ta(e){return e!==void 0}function uC(e,n){return e==null||e===""||(typeof n=="string"?e=e+n:typeof e=="object"&&(e=ps(Jt(e)))),e}function xg(e,n){return(e.flags&(n?8:16))!==0}function p(e,n=""){let t=q(),i=De(),r=e+xe,o=i.firstCreatePass?cr(i,r,1,n,null):i.data[r],s=fC(i,t,o,n);t[r]=s,Os()&&Ld(i,t,s,o),Qi(o,!1)}var fC=(e,n,t,i)=>(Rs(!0),g0(n[ge],i));function mC(e,n,t,i=""){return pi(e,Xi(),t)?n+Jc(t)+i:xt}function no(e){return io("",e),no}function io(e,n,t){let i=q(),r=mC(i,e,n,t);return r!==xt&&pC(i,xn(),r),io}function pC(e,n,t){let i=ul(n,e);v0(e[ge],i,t)}function Ap(e,n,t){let i=De();i.firstCreatePass&&Cg(n,i.data,i.blueprint,At(e),t)}function Cg(e,n,t,i,r){if(e=Ae(e),Array.isArray(e))for(let o=0;o<e.length;o++)Cg(e[o],n,t,i,r);else{let o=De(),s=q(),a=Ne(),c=Yn(e)?e:Ae(e.provide),l=sl(e),d=a.providerIndexes&1048575,h=a.directiveStart,v=a.providerIndexes>>20;if(Yn(e)||!e.multi){let g=new li(l,r,lr,null),_=Ul(c,n,r?d:d+v,h);_===-1?(Gl(qs(a,s),o,c),Hl(o,e,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(g),s.push(g)):(t[_]=g,s[_]=g)}else{let g=Ul(c,n,d+v,h),_=Ul(c,n,d,d+v),b=g>=0&&t[g],I=_>=0&&t[_];if(r&&!I||!r&&!b){Gl(qs(a,s),o,c);let G=vC(r?gC:hC,t.length,r,i,l,e);!r&&I&&(t[_].providerFactory=G),Hl(o,e,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(G),s.push(G)}else{let G=Dg(t[r?_:g],l,!r&&i);Hl(o,e,g>-1?g:_,G)}!r&&i&&I&&t[_].componentProviders++}}}function Hl(e,n,t,i){let r=Yn(n),o=xm(n);if(r||o){let c=(o?Ae(n.useClass):n).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(t);d===-1?l.push(t,[i,c]):l[d+1].push(i,c)}else l.push(t,c)}}}function Dg(e,n,t){return t&&e.componentProviders++,e.multi.push(n)-1}function Ul(e,n,t,i){for(let r=t;r<i;r++)if(n[r]===e)return r;return-1}function hC(e,n,t,i,r){return bd(this.multi,[])}function gC(e,n,t,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=qr(i,i[E],this.providerFactory.index,r);s=c.slice(0,a),bd(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],bd(o,s);return s}function bd(e,n){for(let t=0;t<e.length;t++){let i=e[t];n.push(i())}return n}function vC(e,n,t,i,r,o){let s=new li(e,t,lr,null);return s.multi=[],s.index=n,s.componentProviders=0,Dg(s,r,i&&!t),s}function Ce(e,n){return t=>{t.providersResolver=(i,r)=>Ap(i,r?r(e):e,!1),n&&(t.viewProvidersResolver=(i,r)=>Ap(i,r?r(n):n,!0))}}function ro(e,n){return ua(e,n)}var Eg=(()=>{class e{applicationErrorHandler=m(Kt);appRef=m(Ct);taskService=m(ri);ngZone=m(C);zonelessEnabled=m(Ur);tracing=m(Vt,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new se;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Rr):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(m(Rl,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Ym:kl;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Rr+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();function Ig(){return[{provide:St,useExisting:Eg},{provide:C,useClass:Nr},{provide:Ur,useValue:!0}]}function _C(){return typeof $localize<"u"&&$localize.locale||to}var ha=new y("",{factory:()=>m(ha,{optional:!0,skipSelf:!0})||_C()});function dr(e,n){return wr(e,n?.equal)}var Ag=Symbol("InputSignalNode#UNSET"),LC=ye(V({},Cr),{transformFn:void 0,applyValueToInputSignal(e,n){xr(e,n)}});function Og(e,n){let t=Object.create(LC);t.value=e,t.transformFn=n?.transform;function i(){if(Si(t),t.value===Ag){let r=null;throw new D(-950,r)}return t.value}return i[Ie]=t,i}var va=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>xd(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},Rg=(()=>{let e=new y("");return e.__NG_ELEMENT_ID__=n=>{let t=Ne();if(t===null)throw new D(-204,!1);if(t.type&2)return t.value;if(n&8)return null;throw new D(-204,!1)},e})();function Mg(e,n){return Og(e,n)}function VC(e){return Og(Ag,e)}var Ng=(Mg.required=VC,Mg);function Sg(e,n){return Kd(n)}function BC(e,n){return Qd(n)}var so=(Sg.required=BC,Sg);function kg(e,n){return Kd(n)}function jC(e,n){return Qd(n)}var Fg=(kg.required=jC,kg);var zC=1e4;var sB=zC-1e3;var Ge=(()=>{class e{static __NG_ELEMENT_ID__=HC}return e})();function HC(e){return UC(Ne(),q(),(e&16)===16)}function UC(e,n,t){if(Yt(e)&&!t){let i=st(e.index,n);return new Mn(i,i)}else if(e.type&175){let i=n[Ye];return new Mn(i,n)}return null}var ou=new y(""),$C=new y("");function oo(e){return!e.moduleRef}function WC(e){let n=oo(e)?e.r3Injector:e.moduleRef.injector,t=n.get(C);return t.run(()=>{oo(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let i=n.get(Kt),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),oo(e)){let o=()=>n.destroy(),s=e.platformInjector.get(ou);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>e.moduleRef.destroy(),s=e.platformInjector.get(ou);s.add(o),e.moduleRef.onDestroy(()=>{Gr(e.allPlatformModules,e.moduleRef),r.unsubscribe(),s.delete(o)})}return qC(i,t,()=>{let o=n.get(ri),s=o.add(),a=n.get(Jd);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(ha,to);if(hg(c||to),!n.get($C,!0))return oo(e)?n.get(Ct):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(oo(e)){let d=n.get(Ct);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return GC?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{o.remove(s)})})})}var GC;function qC(e,n,t){try{let i=t();return eo(i)?i.catch(r=>{throw n.runOutsideAngular(()=>e(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>e(i)),i}}var ga=null;function ZC(e=[],n){return ee.create({name:n,providers:[{provide:Lr,useValue:"platform"},{provide:ou,useValue:new Set([()=>ga=null])},...e]})}function YC(e=[]){if(ga)return ga;let n=ZC(e);return ga=n,mg(),KC(n),n}function KC(e){let n=e.get(Ns,null);Gi(e,()=>{n?.forEach(t=>t())})}function Pg(e){let{rootComponent:n,appProviders:t,platformProviders:i,platformRef:r}=e;oe(ne.BootstrapApplicationStart);try{let o=r?.injector??YC(i),s=[Ig(),Qm,...t||[]],a=new Kr({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return WC({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{oe(ne.BootstrapApplicationEnd)}}function le(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function ao(e,n=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):n}var ru=Symbol("NOT_SET"),Lg=new Set,QC=ye(V({},Cr),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:ru,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==ru&&!Ti(this))return this.signal;try{for(let r of this.cleanup??Lg)r()}finally{this.cleanup?.clear()}let n=[];e!==void 0&&n.push(e),n.push(this.registerCleanupFn);let t=Pn(this),i;try{i=this.userFn.apply(null,n)}finally{ki(this,t)}return(this.value===ru||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),su=class extends Zr{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,t,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(at),s),this.scheduler=r;for(let a of Rd){let c=t[a];if(c===void 0)continue;let l=Object.create(QC);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Si(l),l.value),l.signal[Ie]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let t of n.cleanup??Lg)t()}finally{un(n)}}};function Vg(e,n){let t=n?.injector??m(ee),i=t.get(St),r=t.get(sa),o=t.get(Vt,null,{optional:!0});r.impl??=t.get(Nd);let s=e;typeof s=="function"&&(s={mixedReadWrite:e});let a=t.get(Ji,null,{optional:!0}),c=new su(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,t,o?.snapshot(null));return r.impl.register(c),c}function _a(e,n){let t=gn(e),i=n.elementInjector||Wi();return new rr(t).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var Bg=null;function rn(){return Bg}function cu(e){Bg??=e}var co=class{},ya=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=X({token:e,factory:()=>m(jg),providedIn:"platform"})}return e})();var jg=(()=>{class e extends ya{_location;_history;_doc=m(P);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return rn().getBaseHref(this._doc)}onPopState(t){let i=rn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=rn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||e)};static \u0275prov=X({token:e,factory:()=>new e,providedIn:"platform"})}return e})();function Ug(e,n){return e?n?e.endsWith("/")?n.startsWith("/")?e+n.slice(1):e+n:n.startsWith("/")?e+n:`${e}/${n}`:e:n}function zg(e){let n=e.search(/#|\?|$/);return e[n-1]==="/"?e.slice(0,n-1)+e.slice(n):e}function Tn(e){return e&&e[0]!=="?"?`?${e}`:e}var ba=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=X({token:e,factory:()=>m(JC),providedIn:"root"})}return e})(),XC=new y(""),JC=(()=>{class e extends ba{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??m(P).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Ug(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+Tn(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,o){let s=this.prepareExternalUrl(r+Tn(o));this._platformLocation.pushState(t,i,s)}replaceState(t,i,r,o){let s=this.prepareExternalUrl(r+Tn(o));this._platformLocation.replaceState(t,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||e)(T(ya),T(XC,8))};static \u0275prov=X({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var wa=(()=>{class e{_subject=new w;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=nD(zg(Hg(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+Tn(i))}normalize(t){return e.stripTrailingSlash(tD(this._basePath,Hg(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Tn(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Tn(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Tn;static joinWithSlash=Ug;static stripTrailingSlash=zg;static \u0275fac=function(i){return new(i||e)(T(ba))};static \u0275prov=X({token:e,factory:()=>eD(),providedIn:"root"})}return e})();function eD(){return new wa(T(ba))}function tD(e,n){if(!e||!n.startsWith(e))return n;let t=n.substring(e.length);return t===""||["/",";","?","#"].includes(t[0])?t:n}function Hg(e){return e.replace(/\/index\.html$/,"")}function nD(e){if(new RegExp("^(https?:)?//").test(e)){let[,t]=e.split(/\/\/[^\/]+/);return t}return e}var lu=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=m(ee);constructor(t){this._viewContainerRef=t}ngOnChanges(t){if(this._shouldRecreateView(t)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(t){return!!t.ngTemplateOutlet||!!t.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(t,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(t,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||e)(lr(tn))};static \u0275dir=$({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Lt]})}return e})();function du(e,n){n=encodeURIComponent(n);for(let t of e.split(";")){let i=t.indexOf("="),[r,o]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var uu="browser";function $g(e){return e===uu}var lo=class{_doc;constructor(n){this._doc=n}manager},xa=(()=>{class e extends lo{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,o){return t.addEventListener(i,r,o),()=>this.removeEventListener(t,i,r,o)}removeEventListener(t,i,r,o){return t.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||e)(T(P))};static \u0275prov=X({token:e,factory:e.\u0275fac})}return e})(),Ea=new y(""),hu=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(s=>{s.manager=this});let r=t.filter(s=>!(s instanceof xa));this._plugins=r.slice().reverse();let o=t.find(s=>s instanceof xa);o&&this._plugins.push(o)}addEventListener(t,i,r,o){return this._findPluginFor(i).addEventListener(t,i,r,o)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(o=>o.supports(t)),!i)throw new D(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||e)(T(Ea),T(C))};static \u0275prov=X({token:e,factory:e.\u0275fac})}return e})(),fu="ng-app-id";function Wg(e){for(let n of e)n.remove()}function Gg(e,n){let t=n.createElement("style");return t.textContent=e,t}function sD(e,n,t,i){let r=e.head?.querySelectorAll(`style[${fu}="${n}"],link[${fu}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(fu),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]});return!0}function pu(e,n){let t=n.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",e),t}var gu=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,o={}){this.doc=t,this.appId=i,this.nonce=r,sD(t,i,this.inline,this.external)&&this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,Gg);i?.forEach(r=>this.addUsage(r,this.external,pu))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let o=i.get(t);o?o.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(Wg(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Wg(t);this.hosts.clear()}addHost(t){if(!this.hosts.has(t)){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,Gg(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,pu(i,this.doc)))}}removeHost(t){this.hosts.delete(t);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===t?o.remove():r.push(o);i.elements=r}}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||e)(T(P),T(oi),T(ai,8),T(si))};static \u0275prov=X({token:e,factory:e.\u0275fac})}return e})(),mu={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},vu=/%COMP%/g;var Zg="%COMP%",aD=`_nghost-${Zg}`,cD=`_ngcontent-${Zg}`,lD=!0,dD=new y("",{factory:()=>lD});function uD(e){return cD.replace(vu,e)}function fD(e){return aD.replace(vu,e)}function Yg(e,n){return n.map(t=>t.replace(vu,e))}var _u=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,o,s,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new uo(t,s,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Da?r.applyToHost(t):r instanceof fo&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,h=this.tracingService;switch(i.encapsulation){case wt.Emulated:o=new Da(c,l,i,this.appId,d,s,a,h);break;case wt.ShadowDom:return new Ca(c,t,i,s,a,this.nonce,h,l);case wt.ExperimentalIsolatedShadowDom:return new Ca(c,t,i,s,a,this.nonce,h);default:o=new fo(c,l,i,d,s,a,h);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||e)(T(hu),T(hi),T(oi),T(dD),T(P),T(C),T(ai),T(Vt,8))};static \u0275prov=X({token:e,factory:e.\u0275fac})}return e})(),uo=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,t,i,r){this.eventManager=n,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,t){return t?this.doc.createElementNS(mu[t]||t,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,t){(qg(n)?n.content:n).appendChild(t)}insertBefore(n,t,i){n&&(qg(n)?n.content:n).insertBefore(t,i)}removeChild(n,t){t.remove()}selectRootElement(n,t){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new D(-5104,!1);return t||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,t,i,r){if(r){t=r+":"+t;let o=mu[r];o?n.setAttributeNS(o,t,i):n.setAttribute(t,i)}else n.setAttribute(t,i)}removeAttribute(n,t,i){if(i){let r=mu[i];r?n.removeAttributeNS(r,t):n.removeAttribute(`${i}:${t}`)}else n.removeAttribute(t)}addClass(n,t){n.classList.add(t)}removeClass(n,t){n.classList.remove(t)}setStyle(n,t,i,r){r&(Pt.DashCase|Pt.Important)?n.style.setProperty(t,i,r&Pt.Important?"important":""):n.style[t]=i}removeStyle(n,t,i){i&Pt.DashCase?n.style.removeProperty(t):n.style[t]=""}setProperty(n,t,i){n!=null&&(n[t]=i)}setValue(n,t){n.nodeValue=t}listen(n,t,i,r){if(typeof n=="string"&&(n=rn().getGlobalEventTarget(this.doc,n),!n))throw new D(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,t,o)),this.eventManager.addEventListener(n,t,o,r)}decoratePreventDefault(n){return t=>{if(t==="__ngUnwrap__")return n;n(t)===!1&&t.preventDefault()}}};function qg(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var Ca=class extends uo{hostEl;sharedStylesHost;shadowRoot;constructor(n,t,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=Yg(i.id,l);for(let h of l){let v=document.createElement("style");s&&v.setAttribute("nonce",s),v.textContent=h,this.shadowRoot.appendChild(v)}let d=i.getExternalStyles?.();if(d)for(let h of d){let v=pu(h,r);s&&v.setAttribute("nonce",s),this.shadowRoot.appendChild(v)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,t){return super.appendChild(this.nodeOrShadowRoot(n),t)}insertBefore(n,t,i){return super.insertBefore(this.nodeOrShadowRoot(n),t,i)}removeChild(n,t){return super.removeChild(null,t)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},fo=class extends uo{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,t,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?Yg(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&di.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Da=class extends fo{contentAttr;hostAttr;constructor(n,t,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,t,i,o,s,a,c,l),this.contentAttr=uD(l),this.hostAttr=fD(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,t){let i=super.createElement(n,t);return super.setAttribute(i,this.contentAttr,""),i}};var Ia=class e extends co{supportsDOMEvents=!0;static makeCurrent(){cu(new e)}onAndCancel(n,t,i,r){return n.addEventListener(t,i,r),()=>{n.removeEventListener(t,i,r)}}dispatchEvent(n,t){n.dispatchEvent(t)}remove(n){n.remove()}createElement(n,t){return t=t||this.getDefaultDocument(),t.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,t){return t==="window"?window:t==="document"?n:t==="body"?n.body:null}getBaseHref(n){let t=mD();return t==null?null:pD(t)}resetBaseElement(){mo=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return du(document.cookie,n)}},mo=null;function mD(){return mo=mo||document.head.querySelector("base"),mo?mo.getAttribute("href"):null}function pD(e){return new URL(e,document.baseURI).pathname}var Kg=["alt","control","meta","shift"],hD={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},gD={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},Qg=(()=>{class e extends lo{constructor(t){super(t)}supports(t){return e.parseEventName(t)!=null}addEventListener(t,i,r,o){let s=e.parseEventName(i),a=e.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>rn().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=e._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),Kg.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(t,i){let r=hD[t.key]||t.key,o="";return i.indexOf("code.")>-1&&(r=t.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Kg.forEach(s=>{if(s!==r){let a=gD[s];a(t)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(t,i,r){return o=>{e.matchEventFullKeyCode(o,t)&&r.runGuarded(()=>i(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||e)(T(P))};static \u0275prov=X({token:e,factory:e.\u0275fac})}return e})();async function yu(e,n,t){let i=V({rootComponent:e},vD(n,t));return Pg(i)}function vD(e,n){return{platformRef:n?.platformRef,appProviders:[...xD,...e?.providers??[]],platformProviders:wD}}function _D(){Ia.makeCurrent()}function yD(){return new Ze}function bD(){return Dd(document),document}var wD=[{provide:si,useValue:uu},{provide:Ns,useValue:_D,multi:!0},{provide:P,useFactory:bD}];var xD=[{provide:Lr,useValue:"root"},{provide:Ze,useFactory:yD},{provide:Ea,useClass:xa,multi:!0},{provide:Ea,useClass:Qg,multi:!0},_u,{provide:hi,useClass:gu},{provide:gu,useExisting:hi},hu,{provide:Se,useExisting:_u},[]];var sn=class e{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(t=>{let i=t.indexOf(":");if(i>0){let r=t.slice(0,i),o=t.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((t,i)=>{this.addHeaderEntry(i,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([t,i])=>{this.setHeaderEntries(t,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let t=this.headers.get(n.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,t){return this.clone({name:n,value:t,op:"a"})}set(n,t){return this.clone({name:n,value:t,op:"s"})}delete(n,t){return this.clone({name:n,value:t,op:"d"})}maybeSetNormalizedName(n,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,n)}init(){this.lazyInit&&(this.lazyInit instanceof e?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(t=>{this.headers.set(t,n.headers.get(t)),this.normalizedNames.set(t,n.normalizedNames.get(t))})}clone(n){let t=new e;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof e?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([n]),t}applyUpdate(n){let t=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,t);let r=(n.op==="a"?this.headers.get(t):void 0)||[];r.push(...i),this.headers.set(t,r);break;case"d":let o=n.value;if(!o)this.headers.delete(t),this.normalizedNames.delete(t);else{let s=this.headers.get(t);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,s)}break}}addHeaderEntry(n,t){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(t):this.headers.set(i,[t])}setHeaderEntries(n,t){let i=(Array.isArray(t)?t:[t]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>n(this.normalizedNames.get(t),this.headers.get(t)))}};var xu=class{map=new Map;set(n,t){return this.map.set(n,t),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Cu=class{encodeKey(n){return Xg(n)}encodeValue(n){return Xg(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function CD(e,n){let t=new Map;return e.length>0&&e.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=t.get(s)||[];c.push(a),t.set(s,c)}),t}var DD=/%(\d[a-f0-9])/gi,ED={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function Xg(e){return encodeURIComponent(e).replace(DD,(n,t)=>ED[t]??n)}function Ma(e){return`${e}`}var on=class e{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Cu,n.fromString){if(n.fromObject)throw new D(2805,!1);this.map=CD(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(t=>{let i=n.fromObject[t],r=Array.isArray(i)?i.map(Ma):[Ma(i)];this.map.set(t,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let t=this.map.get(n);return t?t[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,t){return this.clone({param:n,value:t,op:"a"})}appendAll(n){let t=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{t.push({param:i,value:o,op:"a"})}):t.push({param:i,value:r,op:"a"})}),this.clone(t)}set(n,t){return this.clone({param:n,value:t,op:"s"})}delete(n,t){return this.clone({param:n,value:t,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let t=this.encoder.encodeKey(n);return this.map.get(n).map(i=>t+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let t=new e({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(n),t}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let t=(n.op==="a"?this.map.get(n.param):void 0)||[];t.push(Ma(n.value)),this.map.set(n.param,t);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Ma(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function ID(e){switch(e){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function Jg(e){return typeof ArrayBuffer<"u"&&e instanceof ArrayBuffer}function ev(e){return typeof Blob<"u"&&e instanceof Blob}function tv(e){return typeof FormData<"u"&&e instanceof FormData}function MD(e){return typeof URLSearchParams<"u"&&e instanceof URLSearchParams}var bu="Content-Type",nv="Accept",rv="text/plain",ov="application/json",SD=`${ov}, ${rv}, */*`,ur=class e{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,t,i,r){this.url=t,this.method=n.toUpperCase();let o;if(ID(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new D(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new sn,this.context??=new xu,!this.params)this.params=new on,this.urlWithParams=t;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=t;else{let a=t,c="",l=t.indexOf("#");l!==-1&&(c=t.substring(l),a=t.substring(0,l));let d=a.indexOf("?"),h=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+h+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||Jg(this.body)||ev(this.body)||tv(this.body)||MD(this.body)?this.body:this.body instanceof on?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||tv(this.body)?null:ev(this.body)?this.body.type||null:Jg(this.body)?null:typeof this.body=="string"?rv:this.body instanceof on?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?ov:null}clone(n={}){let t=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,h=n.referrer??this.referrer,v=n.integrity||this.integrity,g=n.referrerPolicy||this.referrerPolicy,_=n.transferCache??this.transferCache,b=n.timeout??this.timeout,I=n.body!==void 0?n.body:this.body,G=n.withCredentials??this.withCredentials,qe=n.reportProgress??this.reportProgress,dn=n.reportUploadProgress??this.reportUploadProgress,Ei=n.reportDownloadProgress??this.reportDownloadProgress,yr=n.headers||this.headers,Rn=n.params||this.params,Eo=n.context??this.context;return n.setHeaders!==void 0&&(yr=Object.keys(n.setHeaders).reduce((Ii,Nn)=>Ii.set(Nn,n.setHeaders[Nn]),yr)),n.setParams&&(Rn=Object.keys(n.setParams).reduce((Ii,Nn)=>Ii.set(Nn,n.setParams[Nn]),Rn)),new e(t,i,I,{params:Rn,headers:yr,context:Eo,reportProgress:qe,reportUploadProgress:dn,reportDownloadProgress:Ei,responseType:r,withCredentials:G,transferCache:_,keepalive:o,cache:a,priority:s,timeout:b,mode:c,redirect:l,credentials:d,referrer:h,integrity:v,referrerPolicy:g})}},fr=(function(e){return e[e.Sent=0]="Sent",e[e.UploadProgress=1]="UploadProgress",e[e.ResponseHeader=2]="ResponseHeader",e[e.DownloadProgress=3]="DownloadProgress",e[e.Response=4]="Response",e[e.User=5]="User",e})(fr||{}),po=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,t=200,i="OK"){this.headers=n.headers||new sn,this.status=n.status!==void 0?n.status:t,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Du=class e extends po{constructor(n={}){super(n)}type=fr.ResponseHeader;clone(n={}){return new e({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},ho=class e extends po{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=fr.Response;clone(n={}){return new e({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},gi=class extends po{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},kD=200;var TD=/^\)\]\}',?\n/,Ez=1024*1024,AD=new y("",{factory:()=>null}),OD=(()=>{class e{fetchImpl=m(Eu,{optional:!0})?.fetch??((...t)=>globalThis.fetch(...t));ngZone=m(C);destroyRef=m(at);maxResponseSize=m(AD);handle(t){return new Q(i=>{let r=new AbortController;this.doRequest(t,r.signal,i).then(Iu,s=>i.error(new gi({error:s})));let o;return t.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},t.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}async doRequest(t,i,r){let o=this.createRequestInit(t),s;try{let I=this.ngZone.runOutsideAngular(()=>this.fetchImpl(t.urlWithParams,V({signal:i},o)));RD(I),r.next({type:fr.Sent}),s=await I}catch(I){r.error(new gi({error:I,status:I.status??0,statusText:I.statusText,url:t.urlWithParams,headers:I.headers}));return}let a=new sn(s.headers),c=s.statusText,l=s.url||t.urlWithParams,d=s.status,h=null,v=t.reportProgress||t.reportDownloadProgress;if(v&&r.next(new Du({headers:a,status:d,statusText:c,url:l})),s.body){let I=s.headers.get("content-length"),G=I!==null?Number(I):NaN;this.maxResponseSize!==null&&Number.isFinite(G)&&G>this.maxResponseSize&&iv(this.maxResponseSize);let qe=[],dn=s.body.getReader(),Ei=0,yr,Rn,Eo=typeof Zone<"u"&&Zone.current,Ii=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await dn.cancel(),Ii=!0;break}let{done:br,value:oc}=await dn.read();if(br)break;if(qe.push(oc),Ei+=oc.length,this.maxResponseSize!==null&&Ei>this.maxResponseSize&&(await dn.cancel(),iv(this.maxResponseSize)),v){Rn=t.responseType==="text"?(Rn??"")+(yr??=new TextDecoder).decode(oc,{stream:!0}):void 0;let vf=()=>r.next({type:fr.DownloadProgress,total:Number.isFinite(G)?G:void 0,loaded:Ei,partialText:Rn});Eo?Eo.run(vf):vf()}}}),Ii){r.complete();return}let Nn=this.concatChunks(qe,Ei);try{let br=s.headers.get(bu)??"";h=this.parseBody(t,Nn,br,d)}catch(br){r.error(new gi({error:br,headers:new sn(s.headers),status:s.status,statusText:s.statusText,url:s.url||t.urlWithParams}));return}}d===0&&(d=h?kD:0);let g=d>=200&&d<300,_=s.redirected,b=s.type;g?(r.next(new ho({body:h,headers:a,status:d,statusText:c,url:l,redirected:_,responseType:b})),r.complete()):r.error(new gi({error:h,headers:a,status:d,statusText:c,url:l,redirected:_,responseType:b}))}parseBody(t,i,r,o){switch(t.responseType){case"json":let s=new TextDecoder().decode(i).replace(TD,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(t){if(t.reportUploadProgress)throw new D(2824,!1);let i={},r;if(r=t.credentials,t.withCredentials&&(r="include"),t.headers.forEach((o,s)=>i[o]=s.join(",")),t.headers.has(nv)||(i[nv]=SD),!t.headers.has(bu)){let o=t.detectContentTypeHeader();o!==null&&(i[bu]=o)}return{body:t.serializeBody(),method:t.method,headers:i,credentials:r,keepalive:t.keepalive,cache:t.cache,priority:t.priority,mode:t.mode,redirect:t.redirect,referrer:t.referrer,integrity:t.integrity,referrerPolicy:t.referrerPolicy}}concatChunks(t,i){let r=new Uint8Array(i),o=0;for(let s of t)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),Eu=class{};function Iu(){}function RD(e){e.then(Iu,Iu)}function iv(e){throw new D(2825,!1)}function ND(e,n){return n(e)}function FD(e,n,t){return(i,r)=>Gi(t,()=>n(i,o=>e(o,r)))}var PD=new y("",{factory:()=>[]}),sv=new y(""),LD=new y("",{factory:()=>!0});var VD=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=X({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=T(OD),r},providedIn:"root"})}return e})();var BD=(()=>{class e{backend;injector;chain=null;pendingTasks=m(Ps);contributeToStability=m(LD);constructor(t,i){this.backend=t,this.injector=i}handle(t){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(PD),...this.injector.get(sv,[])]));this.chain=i.reduceRight((r,o)=>FD(r,o,this.injector),ND)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(t,r=>this.backend.handle(r)).pipe(Mr(i))}else return this.chain(t,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||e)(T(VD),T(be))};static \u0275prov=X({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),jD=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=X({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=T(BD),r},providedIn:"root"})}return e})();function wu(e,n){return V({body:n},e)}var Mu=(()=>{class e{handler;constructor(t){this.handler=t}request(t,i,r={}){let o;if(t instanceof ur)o=t;else{let c;r.headers instanceof sn?c=r.headers:c=new sn(r.headers);let l;r.params&&(r.params instanceof on?l=r.params:l=new on({fromObject:r.params})),o=new ur(t,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=Xe(o).pipe(kc(c=>this.handler.handle(c)));if(t instanceof ur||r.observe==="events")return s;let a=s.pipe(ve(c=>c instanceof ho));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ae(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new D(2806,!1);return c.body}));case"blob":return a.pipe(ae(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new D(2807,!1);return c.body}));case"text":return a.pipe(ae(c=>{if(c.body!==null&&typeof c.body!="string")throw new D(2808,!1);return c.body}));default:return a.pipe(ae(c=>c.body))}case"response":return a;default:throw new D(2809,!1)}}delete(t,i={}){return this.request("DELETE",t,i)}get(t,i={}){return this.request("GET",t,i)}head(t,i={}){return this.request("HEAD",t,i)}jsonp(t,i){return this.request("JSONP",t,{params:new on().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,i={}){return this.request("OPTIONS",t,i)}patch(t,i,r={}){return this.request("PATCH",t,wu(r,i))}post(t,i,r={}){return this.request("POST",t,wu(r,i))}put(t,i,r={}){return this.request("PUT",t,wu(r,i))}static \u0275fac=function(i){return new(i||e)(T(jD))};static \u0275prov=X({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Su=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=X({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=T(HD),r},providedIn:"root"})}return e})(),HD=(()=>{class e extends Su{_doc=m(P);sanitize(t,i){if(i==null)return null;switch(t){case We.NONE:return i;case We.HTML:return mi(i,"HTML")?Jt(i):Td(this._doc,String(i)).toString();case We.STYLE:return mi(i,"Style")?Jt(i):i;case We.SCRIPT:if(mi(i,"Script"))return Jt(i);throw new D(5200,!1);case We.URL:return mi(i,"URL")?Jt(i):ra(String(i));case We.RESOURCE_URL:if(mi(i,"ResourceURL"))return Jt(i);throw new D(5201,!1);default:throw new D(5202,!1)}}bypassSecurityTrustHtml(t){return Ed(t)}bypassSecurityTrustStyle(t){return Id(t)}bypassSecurityTrustScript(t){return Md(t)}bypassSecurityTrustUrl(t){return Sd(t)}bypassSecurityTrustResourceUrl(t){return kd(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var av={providers:[Ol()]};var ku;try{ku=typeof Intl<"u"&&Intl.v8BreakIterator}catch{ku=!1}var me=(()=>{class e{_platformId=m(si);isBrowser=this._platformId?$g(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||ku)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var Et=(function(e){return e[e.NORMAL=0]="NORMAL",e[e.NEGATED=1]="NEGATED",e[e.INVERTED=2]="INVERTED",e})(Et||{}),Sa,vi;function ka(){if(vi==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return vi=!1,vi;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)vi=!0;else{let e=Element.prototype.scrollTo;e?vi=!/\{\s*\[native code\]\s*\}/.test(e.toString()):vi=!1}}return vi}function mr(){if(typeof document!="object"||!document)return Et.NORMAL;if(Sa==null){let e=document.createElement("div"),n=e.style;e.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let t=document.createElement("div"),i=t.style;i.width="2px",i.height="1px",e.appendChild(t),document.body.appendChild(e),Sa=Et.NORMAL,e.scrollLeft===0&&(e.scrollLeft=1,Sa=e.scrollLeft===0?Et.NEGATED:Et.INVERTED),e.remove()}return Sa}var Tu;function cv(){if(Tu==null){let e=typeof document<"u"?document.head:null;Tu=!!(e&&(e.createShadowRoot||e.attachShadow))}return Tu}function Au(e){if(cv()){let n=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Qe(e){if(e.composedPath)try{return e.composedPath()[0]}catch{}return e.target}function Ou(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var go;function lv(){if(go==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>go=!0}))}finally{go=go||!1}return go}function pr(e){return lv()?e:!!e.capture}var WD=new y("cdk-dir-doc",{providedIn:"root",factory:()=>m(P)}),GD=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function dv(e){let n=e?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?GD.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var dt=(()=>{class e{get value(){return this.valueSignal()}valueSignal=Ee("ltr");change=new ce;constructor(){let t=m(WD,{optional:!0});if(t){let i=t.body?t.body.dir:null,r=t.documentElement?t.documentElement.dir:null;this.valueSignal.set(dv(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var J=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({})}return e})();var uv=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J]})}return e})();function vo(e){return e.buttons===0||e.detail===0}function _o(e){let n=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}function an(e,n=0){return fv(e)?Number(e):arguments.length===2?n:0}function fv(e){return!isNaN(parseFloat(e))&&!isNaN(Number(e))}function ut(e){return e instanceof B?e.nativeElement:e}var mv=new y("cdk-input-modality-detector-options"),pv={ignoreKeys:[18,17,224,91,16]},hv=650,Ru={passive:!0,capture:!0},gv=(()=>{class e{_platform=m(me);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new zn(null);_options;_lastTouchMs=0;_onKeydown=t=>{this._options?.ignoreKeys?.some(i=>i===t.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Qe(t))};_onMousedown=t=>{Date.now()-this._lastTouchMs<hv||(this._modality.next(vo(t)?"keyboard":"mouse"),this._mostRecentTarget=Qe(t))};_onTouchstart=t=>{if(_o(t)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Qe(t)};constructor(){let t=m(C),i=m(P),r=m(mv,{optional:!0});if(this._options=V(V({},pv),r),this.modalityDetected=this._modality.pipe(Rc(1)),this.modalityChanged=this.modalityDetected.pipe(rs()),this._platform.isBrowser){let o=m(Se).createRenderer(null,null);this._listenerCleanups=t.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Ru),o.listen(i,"mousedown",this._onMousedown,Ru),o.listen(i,"touchstart",this._onTouchstart,Ru)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(t=>t())}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),yo=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(yo||{}),vv=new y("cdk-focus-monitor-default-options"),Ta=pr({passive:!0,capture:!0}),_i=(()=>{class e{_ngZone=m(C);_platform=m(me);_inputModalityDetector=m(gv);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=m(P);_stopInputModalityDetector=new w;constructor(){let t=m(vv,{optional:!0});this._detectionMode=t?.detectionMode||yo.IMMEDIATE}_rootNodeFocusAndBlurListener=t=>{let i=Qe(t);for(let r=i;r;r=r.parentElement)t.type==="focus"?this._onFocus(t,r):this._onBlur(t,r)};monitor(t,i=!1){let r=ut(t);if(!this._platform.isBrowser||r.nodeType!==1)return Xe();let o=Au(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new w,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(t){let i=ut(t),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(t,i,r){let o=ut(t),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((t,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(t){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(t)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:t&&this._isLastInteractionFromInputLabel(t)?"mouse":"program"}_shouldBeAttributedToTouch(t){return this._detectionMode===yo.EVENTUAL||!!t?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(t,i){t.classList.toggle("cdk-focused",!!i),t.classList.toggle("cdk-touch-focused",i==="touch"),t.classList.toggle("cdk-keyboard-focused",i==="keyboard"),t.classList.toggle("cdk-mouse-focused",i==="mouse"),t.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(t,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=t,this._originFromTouchInteraction=t==="touch"&&i,this._detectionMode===yo.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?hv:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(t,i){let r=this._elementInfo.get(i),o=Qe(t);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(t,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&t.relatedTarget instanceof Node&&i.contains(t.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(t,i){t.subject.observers.length&&this._ngZone.run(()=>t.subject.next(i))}_registerGlobalListeners(t){if(!this._platform.isBrowser)return;let i=t.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Ta),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Ta)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(pe(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(t){let i=t.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Ta),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Ta),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(t,i,r){this._setClasses(t,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(t){let i=[];return this._elementInfo.forEach((r,o)=>{(o===t||r.checkChildren&&o.contains(t))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(t){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===t||t.nodeName!=="INPUT"&&t.nodeName!=="TEXTAREA"||t.disabled)return!1;let o=t.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var Aa=new WeakMap,Le=(()=>{class e{_appRef;_injector=m(ee);_environmentInjector=m(be);load(t){let i=this._appRef=this._appRef||this._injector.get(Ct),r=Aa.get(i);r||(r={loaders:new Set,refs:[]},Aa.set(i,r),i.onDestroy(()=>{Aa.get(i)?.refs.forEach(o=>o.destroy()),Aa.delete(i)})),r.loaders.has(t)||(r.loaders.add(t),r.refs.push(_a(t,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var hr=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return e})(),Oa;function qD(){if(Oa===void 0&&(Oa=null,typeof window<"u")){let e=window;e.trustedTypes!==void 0&&(Oa=e.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Oa}function gr(e){return qD()?.createHTML(e)||e}function Nu(e){return Array.isArray(e)?e:[e]}var _v=new Set,yi,Fu=(()=>{class e{_platform=m(me);_nonce=m(ai,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):YD}matchMedia(t){return(this._platform.WEBKIT||this._platform.BLINK)&&ZD(t,this._nonce),this._matchMedia(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();function ZD(e,n){if(!_v.has(e))try{yi||(yi=document.createElement("style"),n&&yi.setAttribute("nonce",n),yi.setAttribute("type","text/css"),document.head.appendChild(yi)),yi.sheet&&(yi.sheet.insertRule(`@media ${e.replace(/[{}]/g,"")} {body{ }}`,0),_v.add(e))}catch(t){console.error(t)}}function YD(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}function KD(e){if(e.type==="characterData"&&e.target instanceof Comment)return!0;if(e.type==="childList"){for(let n=0;n<e.addedNodes.length;n++)if(!(e.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<e.removedNodes.length;n++)if(!(e.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var yv=(()=>{class e{create(t){return typeof MutationObserver>"u"?null:new MutationObserver(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),QD=(()=>{class e{_mutationObserverFactory=m(yv);_observedElements=new Map;_ngZone=m(C);ngOnDestroy(){this._observedElements.forEach((t,i)=>this._cleanupObserver(i))}observe(t){let i=ut(t);return new Q(r=>{let s=this._observeElement(i).pipe(ae(a=>a.filter(c=>!KD(c))),ve(a=>!!a.length)).subscribe(a=>{this._ngZone.run(()=>{r.next(a)})});return()=>{s.unsubscribe(),this._unobserveElement(i)}})}_observeElement(t){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(t))this._observedElements.get(t).count++;else{let i=new w,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(t,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(t,{observer:r,stream:i,count:1})}return this._observedElements.get(t).stream})}_unobserveElement(t){this._observedElements.has(t)&&(this._observedElements.get(t).count--,this._observedElements.get(t).count||this._cleanupObserver(t))}_cleanupObserver(t){if(this._observedElements.has(t)){let{observer:i,stream:r}=this._observedElements.get(t);i&&i.disconnect(),r.complete(),this._observedElements.delete(t)}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),bv=(()=>{class e{_contentObserver=m(QD);_elementRef=m(B);event=new ce;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(t){this._debounce=an(t),this._subscribe()}_debounce;_currentSubscription=null;ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let t=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?t.pipe($n(this.debounce)):t).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",le],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return e})(),Ra=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({providers:[yv]})}return e})();var Fa=(()=>{class e{_platform=m(me);isDisabled(t){return t.hasAttribute("disabled")}isVisible(t){return JD(t)&&getComputedStyle(t).visibility==="visible"}isTabbable(t){if(!this._platform.isBrowser)return!1;let i=XD(aE(t));if(i&&(wv(i)===-1||!this.isVisible(i)))return!1;let r=t.nodeName.toLowerCase(),o=wv(t);return t.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!oE(t)?!1:r==="audio"?t.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||t.hasAttribute("controls"):t.tabIndex>=0}isFocusable(t,i){return sE(t)&&!this.isDisabled(t)&&(i?.ignoreVisibility||this.isVisible(t))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();function XD(e){try{return e.frameElement}catch{return null}}function JD(e){return!!(e.offsetWidth||e.offsetHeight||typeof e.getClientRects=="function"&&e.getClientRects().length)}function eE(e){let n=e.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function tE(e){return iE(e)&&e.type=="hidden"}function nE(e){return rE(e)&&e.hasAttribute("href")}function iE(e){return e.nodeName.toLowerCase()=="input"}function rE(e){return e.nodeName.toLowerCase()=="a"}function xv(e){if(!e.hasAttribute("tabindex")||e.tabIndex===void 0)return!1;let n=e.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function wv(e){if(!xv(e))return null;let n=parseInt(e.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function oE(e){let n=e.nodeName.toLowerCase(),t=n==="input"&&e.type;return t==="text"||t==="password"||n==="select"||n==="textarea"}function sE(e){return tE(e)?!1:eE(e)||nE(e)||e.hasAttribute("contenteditable")||xv(e)}function aE(e){return e.ownerDocument&&e.ownerDocument.defaultView||window}var Na=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,t,i,r,o=!1,s){this._element=n,this._checker=t,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,t=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),t&&(t.removeEventListener("focus",this.endAnchorListener),t.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(t=>{this._executeOnStable(()=>t(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(t=>{this._executeOnStable(()=>t(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(t=>{this._executeOnStable(()=>t(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let t=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?t.length?t[0]:this._getFirstTabbableElement(this._element):t.length?t[t.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let t=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(t){if(!this._checker.isFocusable(t)){let i=this._getFirstTabbableElement(t);return i?.focus(n),!!i}return t.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let t=this._getRegionBoundary("start");return t&&t.focus(n),!!t}focusLastTabbableElement(n){let t=this._getRegionBoundary("end");return t&&t.focus(n),!!t}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let t=n.children;for(let i=0;i<t.length;i++){let r=t[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(t[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let t=n.children;for(let i=t.length-1;i>=0;i--){let r=t[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(t[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,t){n?t.setAttribute("tabindex","0"):t.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){ct(n,{injector:this._injector})}},Pu=(()=>{class e{_checker=m(Fa);_ngZone=m(C);_document=m(P);_injector=m(ee);constructor(){m(Le).load(hr)}create(t,i=!1){return new Na(t,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var cE=200,Pa=class{_letterKeyStream=new w;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new w;selectedItem=this._selectedItem;constructor(n,t){let i=typeof t?.debounceInterval=="number"?t.debounceInterval:cE;t?.skipPredicate&&(this._skipPredicateFn=t.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let t=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(t>=65&&t<=90||t>=48&&t<=57)&&this._letterKeyStream.next(String.fromCharCode(t))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Wn(t=>this._pressedLetters.push(t)),$n(n),ve(()=>this._pressedLetters.length>0),ae(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(t=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(t)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function vr(e,...n){return n.length?n.some(t=>e[t]):e.altKey||e.shiftKey||e.ctrlKey||e.metaKey}var La=class{_items;_activeItemIndex=Ee(-1);_activeItem=Ee(null);_wrap=!1;_typeaheadSubscription=se.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,t){this._items=n,n instanceof bt?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):En(n)&&(this._effectRef=ci(()=>this._itemsChanged(n()),{injector:t}))}tabOut=new w;change=new w;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let t=this._getItemsArray();return this._typeahead=new Pa(t,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,t=10){return this._pageUpAndDown={enabled:n,delta:t},this}setActiveItem(n){let t=this._activeItem();this.updateActiveItem(n),this._activeItem()!==t&&this.change.next(this._activeItemIndex())}onKeydown(n){let t=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(t){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||vr(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let t=this._getItemsArray(),i=typeof n=="number"?n:t.indexOf(n),r=t[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let t=this._getItemsArray();for(let i=1;i<=t.length;i++){let r=(this._activeItemIndex()+n*i+t.length)%t.length,o=t[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,t){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=t,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return En(this._items)?this._items():this._items instanceof bt?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let t=this._activeItem();if(t){let i=n.indexOf(t);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var bo=class extends La{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var Cv=new Map,ft=class e{_appId=m(oi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,t=!1){this._appId!=="ng"&&(n+=this._appId);let i=Cv.get(n);return i===void 0?i=0:i++,Cv.set(n,i),`${n}${t?e._infix+"-":""}${i}`}static \u0275fac=function(t){return new(t||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})};function we(e){return e==null?"":typeof e=="string"?e:`${e}px`}function Te(e){return e!=null&&`${e}`!="false"}var dE=20,Va=(()=>{class e{_ngZone=m(C);_platform=m(me);_renderer=m(Se).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new w;_scrolledCount=0;scrollContainers=new Map;register(t){this.scrollContainers.has(t)||this.scrollContainers.set(t,t.elementScrolled().subscribe(()=>this._scrolled.next(t)))}deregister(t){let i=this.scrollContainers.get(t);i&&(i.unsubscribe(),this.scrollContainers.delete(t))}scrolled(t=dE){return this._platform.isBrowser?new Q(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=t>0?this._scrolled.pipe(ns(t)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Xe()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((t,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(t,i){let r=this.getAncestorScrollContainers(t);return this.scrolled(i).pipe(ve(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(t){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,t)&&i.push(o)}),i}_targetContainsElement(t,i){let r=ut(i),o=t.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),_r=(()=>{class e{elementRef=m(B);scrollDispatcher=m(Va);ngZone=m(C);dir=m(dt,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new w;_renderer=m(Ke);_cleanupScroll;_elementScrolled=new w;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",t=>this._elementScrolled.next(t))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(t){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";t.left==null&&(t.left=r?t.end:t.start),t.right==null&&(t.right=r?t.start:t.end),t.bottom!=null&&(t.top=i.scrollHeight-i.clientHeight-t.bottom),r&&mr()!=Et.NORMAL?(t.left!=null&&(t.right=i.scrollWidth-i.clientWidth-t.left),mr()==Et.INVERTED?t.left=t.right:mr()==Et.NEGATED&&(t.left=t.right?-t.right:t.right)):t.right!=null&&(t.left=i.scrollWidth-i.clientWidth-t.right),this._applyScrollToOptions(t)}_applyScrollToOptions(t){let i=this.elementRef.nativeElement;ka()?i.scrollTo(t):(t.top!=null&&(i.scrollTop=t.top),t.left!=null&&(i.scrollLeft=t.left))}measureScrollOffset(t){let i="left",r="right",o=this.elementRef.nativeElement;if(t=="top")return o.scrollTop;if(t=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let s=this.dir&&this.dir.value=="rtl";return t=="start"?t=s?r:i:t=="end"&&(t=s?i:r),s&&mr()==Et.INVERTED?t==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:s&&mr()==Et.NEGATED?t==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:t==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return e})(),uE=20,An=(()=>{class e{_platform=m(me);_listeners;_viewportSize=null;_change=new w;_document=m(P);constructor(){let t=m(C),i=m(Se).createRenderer(null,null);t.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(t=>t()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let t={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),t}getViewportRect(){let t=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:t.top,left:t.left,bottom:t.top+r,right:t.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let t=this._document,i=this._getWindow(),r=t.documentElement,o=r.getBoundingClientRect(),s=-o.top||t.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||t.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(t=uE){return t>0?this._change.pipe(ns(t)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let t=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:t.innerWidth,height:t.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var bi=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({})}return e})(),Lu=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J,bi,J,bi]})}return e})();var fE=new y("MATERIAL_ANIMATIONS"),Dv=null;function mE(){return m(fE,{optional:!0})?.animationsDisabled||m(Hr,{optional:!0})==="NoopAnimations"?"di-disabled":(Dv??=m(Fu).matchMedia("(prefers-reduced-motion)").matches,Dv?"reduced-motion":"enabled")}function Ue(){return mE()!=="enabled"}var ja=["*"],hE=["content"],Ev=[[["mat-drawer"],["mat-sidenav"]],[["mat-drawer-content"],["mat-sidenav-content"]],"*"],Iv=["mat-drawer, mat-sidenav","mat-drawer-content, mat-sidenav-content","*"];function gE(e,n){if(e&1){let t=Sn();u(0,"div",1),ue("click",function(){Rt(t);let r=he();return Nt(r._onBackdropClicked())}),f()}if(e&2){let t=he();N("mat-drawer-shown",t._isShowingBackdrop())}}function vE(e,n){e&1&&(u(0,"mat-drawer-content"),x(1,2),f())}function _E(e,n){if(e&1){let t=Sn();u(0,"div",1),ue("click",function(){Rt(t);let r=he();return Nt(r._onBackdropClicked())}),f()}if(e&2){let t=he();N("mat-drawer-shown",t._isShowingBackdrop())}}function yE(e,n){e&1&&(u(0,"mat-sidenav-content"),x(1,2),f())}var bE=`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`;var wE=new y("MAT_DRAWER_DEFAULT_AUTOSIZE",{providedIn:"root",factory:()=>!1}),ju=new y("MAT_DRAWER_CONTAINER"),wo=(()=>{class e extends _r{_platform=m(me);_changeDetectorRef=m(Ge);_element=m(B);_ngZone=m(C);_isInert=!1;_container=m(Bu);ngAfterContentInit(){this._container._contentMarginChanges.subscribe(()=>this._changeDetectorRef.markForCheck())}_drawerToggled(t){t.opened?this._ngZone.runOutsideAngular(()=>{t._animationEnd.pipe(Ac(50),Ht(1)).subscribe(()=>this._updateInert())}):this._updateInert()}_updateInert(){let t=this._container._isShowingBackdrop();if(t!==this._isInert){let i=this._element.nativeElement;this._isInert=t,t?i.setAttribute("inert","true"):i.removeAttribute("inert")}}_shouldBeHidden(){if(this._platform.isBrowser)return!1;let{start:t,end:i}=this._container;return t!=null&&t.mode!=="over"&&t.opened||i!=null&&i.mode!=="over"&&i.opened}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-drawer-content"]],hostAttrs:[1,"mat-drawer-content"],hostVars:6,hostBindings:function(i,r){i&2&&(kn("margin-left",r._container._contentMargins.left,"px")("margin-right",r._container._contentMargins.right,"px"),N("mat-drawer-content-hidden",r._shouldBeHidden()))},features:[Ce([{provide:_r,useExisting:e}]),_e],ngContentSelectors:ja,decls:1,vars:0,template:function(i,r){i&1&&(re(),x(0))},encapsulation:2})}return e})(),Vu=(()=>{class e{_elementRef=m(B);_focusTrapFactory=m(Pu);_focusMonitor=m(_i);_platform=m(me);_ngZone=m(C);_renderer=m(Ke);_interactivityChecker=m(Fa);_doc=m(P);_container=m(ju,{optional:!0});_focusTrap=null;_elementFocusedBeforeDrawerWasOpened=null;_eventCleanups;_isAttached=!1;_anchor=null;get position(){return this._position}set position(t){t=t==="end"?"end":"start",t!==this._position&&(this._isAttached&&this._updatePositionInParent(t),this._position=t,this.onPositionChanged.emit())}_position="start";get mode(){return this._mode}set mode(t){this._mode=t,this._updateFocusTrapState(),this._modeChanged.next()}_mode="over";get disableClose(){return this._disableClose}set disableClose(t){this._disableClose=Te(t)}_disableClose=!1;get autoFocus(){let t=this._autoFocus;return t??(this.mode==="side"?"dialog":"first-tabbable")}set autoFocus(t){(t==="true"||t==="false"||t==null)&&(t=Te(t)),this._autoFocus=t}_autoFocus;get opened(){return this._opened()}set opened(t){this.toggle(Te(t))}_opened=Ee(!1);_openedVia=null;_animationStarted=new w;_animationEnd=new w;openedChange=new ce(!0);_openedStream=this.openedChange.pipe(ve(t=>t),ae(()=>{}));openedStart=this._animationStarted.pipe(ve(()=>this.opened),Vi(void 0));_closedStream=this.openedChange.pipe(ve(t=>!t),ae(()=>{}));closedStart=this._animationStarted.pipe(ve(()=>!this.opened),Vi(void 0));_destroyed=new w;onPositionChanged=new ce;_content;_modeChanged=new w;_injector=m(ee);_changeDetectorRef=m(Ge);constructor(){this.openedChange.pipe(pe(this._destroyed)).subscribe(t=>{t?(this._elementFocusedBeforeDrawerWasOpened=this._doc.activeElement,this._takeFocus()):this._isFocusWithinDrawer()&&this._restoreFocus(this._openedVia||"program")}),this._eventCleanups=this._ngZone.runOutsideAngular(()=>{let t=this._renderer,i=this._elementRef.nativeElement;return[t.listen(i,"keydown",r=>{r.keyCode===27&&!this.disableClose&&!vr(r)&&this._ngZone.run(()=>{this.close(),r.stopPropagation(),r.preventDefault()})}),t.listen(i,"transitionend",this._handleTransitionEvent),t.listen(i,"transitioncancel",this._handleTransitionEvent)]}),this._animationEnd.subscribe(()=>{this.openedChange.emit(this.opened)})}_focusByCssSelector(t,i){let r=this._elementRef.nativeElement.querySelector(t);r&&(this._interactivityChecker.isFocusable(r)||(r.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let o=()=>{s(),a(),r.removeAttribute("tabindex")},s=this._renderer.listen(r,"blur",o),a=this._renderer.listen(r,"mousedown",o)})),r.focus(i))}_takeFocus(){if(!this._focusTrap)return;let t=this._elementRef.nativeElement;switch(this.autoFocus){case!1:case"dialog":return;case!0:case"first-tabbable":ct(()=>{!this._focusTrap.focusInitialElement()&&typeof t.focus=="function"&&t.focus()},{injector:this._injector});break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]');break;default:this._focusByCssSelector(this.autoFocus);break}}_restoreFocus(t){this.autoFocus!=="dialog"&&(this._elementFocusedBeforeDrawerWasOpened?this._focusMonitor.focusVia(this._elementFocusedBeforeDrawerWasOpened,t):this._elementRef.nativeElement.blur(),this._elementFocusedBeforeDrawerWasOpened=null)}_isFocusWithinDrawer(){let t=this._doc.activeElement;return!!t&&this._elementRef.nativeElement.contains(t)}ngAfterViewInit(){this._isAttached=!0,this._position==="end"&&this._updatePositionInParent("end"),this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._updateFocusTrapState())}ngOnDestroy(){this._eventCleanups.forEach(t=>t()),this._focusTrap?.destroy(),this._anchor?.remove(),this._anchor=null,this._animationStarted.complete(),this._animationEnd.complete(),this._modeChanged.complete(),this._destroyed.next(),this._destroyed.complete()}open(t){return this.toggle(!0,t)}close(){return this.toggle(!1)}_closeViaBackdropClick(){return this._setOpen(!1,!0,"mouse")}toggle(t=!this.opened,i){t&&i&&(this._openedVia=i);let r=this._setOpen(t,!t&&this._isFocusWithinDrawer(),this._openedVia||"program");return t||(this._openedVia=null),r}_setOpen(t,i,r){return t===this.opened?Promise.resolve(t?"open":"close"):(this._opened.set(t),(this._container?._content||this._container?._userContent)?._drawerToggled(this),this._container?._transitionsEnabled?(this._setIsAnimating(!0),setTimeout(()=>this._animationStarted.next())):setTimeout(()=>{this._animationStarted.next(),this._animationEnd.next()}),this._elementRef.nativeElement.classList.toggle("mat-drawer-opened",t),!t&&i&&this._restoreFocus(r),this._changeDetectorRef.markForCheck(),this._updateFocusTrapState(),new Promise(o=>{this.openedChange.pipe(Ht(1)).subscribe(s=>o(s?"open":"close"))}))}_setIsAnimating(t){this._elementRef.nativeElement.classList.toggle("mat-drawer-animating",t)}_getWidth(){return this._elementRef.nativeElement.offsetWidth||0}_updateFocusTrapState(){this._focusTrap&&(this._focusTrap.enabled=this.opened&&!!this._container?._isShowingBackdrop())}_updatePositionInParent(t){if(!this._platform.isBrowser)return;let i=this._elementRef.nativeElement,r=i.parentNode;t==="end"?(this._anchor||(this._anchor=this._doc.createComment("mat-drawer-anchor"),r.insertBefore(this._anchor,i)),r.appendChild(i)):this._anchor&&this._anchor.parentNode.insertBefore(i,this._anchor)}_handleTransitionEvent=t=>{let i=this._elementRef.nativeElement;t.target===i&&this._ngZone.run(()=>{t.type==="transitionend"&&this._setIsAnimating(!1),this._animationEnd.next(t)})};static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-drawer"]],viewQuery:function(i,r){if(i&1&&Pe(hE,5),i&2){let o;S(o=k())&&(r._content=o.first)}},hostAttrs:[1,"mat-drawer"],hostVars:12,hostBindings:function(i,r){i&2&&(ie("align",null)("tabIndex",r.mode!=="side"?"-1":null),kn("visibility",!r._container&&!r.opened?"hidden":null),N("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side"))},inputs:{position:"position",mode:"mode",disableClose:"disableClose",autoFocus:"autoFocus",opened:"opened"},outputs:{openedChange:"openedChange",_openedStream:"opened",openedStart:"openedStart",_closedStream:"closed",closedStart:"closedStart",onPositionChanged:"positionChanged"},exportAs:["matDrawer"],ngContentSelectors:ja,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(re(),u(0,"div",1,0),x(2),f())},dependencies:[_r],encapsulation:2})}return e})(),Bu=(()=>{class e{_dir=m(dt,{optional:!0});_element=m(B);_ngZone=m(C);_changeDetectorRef=m(Ge);_animationDisabled=Ue();_transitionsEnabled=!1;_allDrawers;_drawers=new bt;_content;_userContent;get start(){return this._start}get end(){return this._end}get autosize(){return this._autosize}set autosize(t){this._autosize=Te(t)}_autosize=m(wE);get hasBackdrop(){return this._drawerHasBackdrop(this._start)||this._drawerHasBackdrop(this._end)}set hasBackdrop(t){this._backdropOverride=t==null?null:Te(t)}_backdropOverride=null;backdropClick=new ce;_start=null;_end=null;_left=null;_right=null;_destroyed=new w;_doCheckSubject=new w;_contentMargins={left:null,right:null};_contentMarginChanges=new w;get scrollable(){return this._userContent||this._content}_injector=m(ee);constructor(){let t=m(me),i=m(An);this._dir?.change.pipe(pe(this._destroyed)).subscribe(()=>{this._validateDrawers(),this.updateContentMargins()}),i.change().pipe(pe(this._destroyed)).subscribe(()=>this.updateContentMargins()),!this._animationDisabled&&t.isBrowser&&this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._element.nativeElement.classList.add("mat-drawer-transition"),this._transitionsEnabled=!0},200)})}ngAfterContentInit(){this._allDrawers.changes.pipe(nt(this._allDrawers),pe(this._destroyed)).subscribe(t=>{this._drawers.reset(t.filter(i=>!i._container||i._container===this)),this._drawers.notifyOnChanges()}),this._drawers.changes.pipe(nt(null)).subscribe(()=>{this._validateDrawers(),this._drawers.forEach(t=>{this._watchDrawerToggle(t),this._watchDrawerPosition(t),this._watchDrawerMode(t)}),(!this._drawers.length||this._isDrawerOpen(this._start)||this._isDrawerOpen(this._end))&&this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),this._ngZone.runOutsideAngular(()=>{this._doCheckSubject.pipe($n(10),pe(this._destroyed)).subscribe(()=>this.updateContentMargins())})}ngOnDestroy(){this._contentMarginChanges.complete(),this._doCheckSubject.complete(),this._drawers.destroy(),this._destroyed.next(),this._destroyed.complete()}open(){this._drawers.forEach(t=>t.open())}close(){this._drawers.forEach(t=>t.close())}updateContentMargins(){let t=0,i=0;if(this._left&&this._left.opened){if(this._left.mode=="side")t+=this._left._getWidth();else if(this._left.mode=="push"){let r=this._left._getWidth();t+=r,i-=r}}if(this._right&&this._right.opened){if(this._right.mode=="side")i+=this._right._getWidth();else if(this._right.mode=="push"){let r=this._right._getWidth();i+=r,t-=r}}t=t||null,i=i||null,(t!==this._contentMargins.left||i!==this._contentMargins.right)&&(this._contentMargins={left:t,right:i},this._ngZone.run(()=>this._contentMarginChanges.next(this._contentMargins)))}ngDoCheck(){this._autosize&&this._isPushed()&&this._ngZone.runOutsideAngular(()=>this._doCheckSubject.next())}_watchDrawerToggle(t){t._animationStarted.pipe(pe(this._drawers.changes)).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),t.mode!=="side"&&t.openedChange.pipe(pe(this._drawers.changes)).subscribe(()=>this._setContainerClass(t.opened))}_watchDrawerPosition(t){t.onPositionChanged.pipe(pe(this._drawers.changes)).subscribe(()=>{ct({read:()=>this._validateDrawers()},{injector:this._injector})})}_watchDrawerMode(t){t._modeChanged.pipe(pe(Mt(this._drawers.changes,this._destroyed))).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()})}_setContainerClass(t){let i=this._element.nativeElement.classList,r="mat-drawer-container-has-open";t?i.add(r):i.remove(r)}_validateDrawers(){this._start=this._end=null,this._drawers.forEach(t=>{t.position=="end"?(this._end!=null,this._end=t):(this._start!=null,this._start=t)}),this._right=this._left=null,this._dir&&this._dir.value==="rtl"?(this._left=this._end,this._right=this._start):(this._left=this._start,this._right=this._end)}_isPushed(){return this._isDrawerOpen(this._start)&&this._start.mode!="over"||this._isDrawerOpen(this._end)&&this._end.mode!="over"}_onBackdropClicked(){this.backdropClick.emit(),this._closeModalDrawersViaBackdrop()}_closeModalDrawersViaBackdrop(){[this._start,this._end].filter(t=>t&&!t.disableClose&&this._drawerHasBackdrop(t)).forEach(t=>t._closeViaBackdropClick())}_isShowingBackdrop(){return this._isDrawerOpen(this._start)&&this._drawerHasBackdrop(this._start)||this._isDrawerOpen(this._end)&&this._drawerHasBackdrop(this._end)}_isDrawerOpen(t){return t!=null&&t.opened}_drawerHasBackdrop(t){return this._backdropOverride==null?!!t&&t.mode!=="side":this._backdropOverride}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-drawer-container"]],contentQueries:function(i,r,o){if(i&1&&He(o,wo,5)(o,Vu,5),i&2){let s;S(s=k())&&(r._content=s.first),S(s=k())&&(r._allDrawers=s)}},viewQuery:function(i,r){if(i&1&&Pe(wo,5),i&2){let o;S(o=k())&&(r._userContent=o.first)}},hostAttrs:[1,"mat-drawer-container"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-drawer-container-explicit-backdrop",r._backdropOverride)},inputs:{autosize:"autosize",hasBackdrop:"hasBackdrop"},outputs:{backdropClick:"backdropClick"},exportAs:["matDrawerContainer"],features:[Ce([{provide:ju,useExisting:e}])],ngContentSelectors:Iv,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(re(Ev),Z(0,gE,1,2,"div",0),x(1),x(2,1),Z(3,vE,2,0,"mat-drawer-content")),i&2&&(Y(r.hasBackdrop?0:-1),z(3),Y(r._content?-1:3))},dependencies:[wo],styles:[`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`],encapsulation:2})}return e})(),Ba=(()=>{class e extends wo{static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-sidenav-content"]],hostAttrs:[1,"mat-drawer-content","mat-sidenav-content"],features:[Ce([{provide:_r,useExisting:e},{provide:wo,useExisting:e}]),_e],ngContentSelectors:ja,decls:1,vars:0,template:function(i,r){i&1&&(re(),x(0))},encapsulation:2})}return e})(),zu=(()=>{class e extends Vu{get fixedInViewport(){return this._fixedInViewport}set fixedInViewport(t){this._fixedInViewport=Te(t)}_fixedInViewport=!1;get fixedTopGap(){return this._fixedTopGap}set fixedTopGap(t){this._fixedTopGap=an(t)}_fixedTopGap=0;get fixedBottomGap(){return this._fixedBottomGap}set fixedBottomGap(t){this._fixedBottomGap=an(t)}_fixedBottomGap=0;static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-sidenav"]],hostAttrs:[1,"mat-drawer","mat-sidenav"],hostVars:16,hostBindings:function(i,r){i&2&&(ie("tabIndex",r.mode!=="side"?"-1":null)("align",null),kn("top",r.fixedInViewport?r.fixedTopGap:null,"px")("bottom",r.fixedInViewport?r.fixedBottomGap:null,"px"),N("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side")("mat-sidenav-fixed",r.fixedInViewport))},inputs:{fixedInViewport:"fixedInViewport",fixedTopGap:"fixedTopGap",fixedBottomGap:"fixedBottomGap"},exportAs:["matSidenav"],features:[Ce([{provide:Vu,useExisting:e}]),_e],ngContentSelectors:ja,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(re(),u(0,"div",1,0),x(2),f())},dependencies:[_r],encapsulation:2})}return e})(),Mv=(()=>{class e extends Bu{_allDrawers=void 0;_content=void 0;static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-sidenav-container"]],contentQueries:function(i,r,o){if(i&1&&He(o,Ba,5)(o,zu,5),i&2){let s;S(s=k())&&(r._content=s.first),S(s=k())&&(r._allDrawers=s)}},hostAttrs:[1,"mat-drawer-container","mat-sidenav-container"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-drawer-container-explicit-backdrop",r._backdropOverride)},exportAs:["matSidenavContainer"],features:[Ce([{provide:ju,useExisting:e},{provide:Bu,useExisting:e}]),_e],ngContentSelectors:Iv,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(re(Ev),Z(0,_E,1,2,"div",0),x(1),x(2,1),Z(3,yE,2,0,"mat-sidenav-content")),i&2&&(Y(r.hasBackdrop?0:-1),z(3),Y(r._content?-1:3))},dependencies:[Ba],styles:[bE],encapsulation:2})}return e})(),Sv=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[bi,J,bi]})}return e})();var mt=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(mt||{}),Hu=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=mt.HIDDEN;constructor(n,t,i,r=!1){this._renderer=n,this.element=t,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},kv=pr({passive:!0,capture:!0}),Uu=class{_events=new Map;addHandler(n,t,i,r){let o=this._events.get(t);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(t,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(t,this._delegateEventHandler,kv)})}removeHandler(n,t,i){let r=this._events.get(n);if(!r)return;let o=r.get(t);o&&(o.delete(i),o.size===0&&r.delete(t),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,kv)))}_delegateEventHandler=n=>{let t=Qe(n);t&&this._events.get(n.type)?.forEach((i,r)=>{(r===t||r.contains(t))&&i.forEach(o=>o.handleEvent(n))})}},xo={enterDuration:225,exitDuration:150},CE=800,Tv=pr({passive:!0,capture:!0}),Av=["mousedown","touchstart"],Ov=["mouseup","mouseleave","touchend","touchcancel"],DE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return e})(),wi=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Uu;constructor(n,t,i,r,o){this._target=n,this._ngZone=t,this._platform=r,r.isBrowser&&(this._containerElement=ut(i)),o&&o.get(Le).load(DE)}fadeInRipple(n,t,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=V(V({},xo),i.animation);i.centered&&(n=r.left+r.width/2,t=r.top+r.height/2);let s=i.radius||EE(n,t,r),a=n-r.left,c=t-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let h=window.getComputedStyle(d),v=h.transitionProperty,g=h.transitionDuration,_=v==="none"||g==="0s"||g==="0s, 0s"||r.width===0&&r.height===0,b=new Hu(this,d,i,_);d.style.transform="scale3d(1, 1, 1)",b.state=mt.FADING_IN,i.persistent||(this._mostRecentTransientRipple=b);let I=null;return!_&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let G=()=>{I&&(I.fallbackTimer=null),clearTimeout(dn),this._finishRippleTransition(b)},qe=()=>this._destroyRipple(b),dn=setTimeout(qe,l+100);d.addEventListener("transitionend",G),d.addEventListener("transitioncancel",qe),I={onTransitionEnd:G,onTransitionCancel:qe,fallbackTimer:dn}}),this._activeRipples.set(b,I),(_||!l)&&this._finishRippleTransition(b),b}fadeOutRipple(n){if(n.state===mt.FADING_OUT||n.state===mt.HIDDEN)return;let t=n.element,i=V(V({},xo),n.config.animation);t.style.transitionDuration=`${i.exitDuration}ms`,t.style.opacity="0",n.state=mt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let t=ut(n);!this._platform.isBrowser||!t||t===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=t,Av.forEach(i=>{e._eventManager.addHandler(this._ngZone,i,t,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Ov.forEach(t=>{this._triggerElement.addEventListener(t,this,Tv)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===mt.FADING_IN?this._startFadeOutTransition(n):n.state===mt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let t=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=mt.VISIBLE,!i&&(!t||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let t=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=mt.HIDDEN,t!==null&&(n.element.removeEventListener("transitionend",t.onTransitionEnd),n.element.removeEventListener("transitioncancel",t.onTransitionCancel),t.fallbackTimer!==null&&clearTimeout(t.fallbackTimer)),n.element.remove()}_onMousedown(n){let t=vo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+CE;!this._target.rippleDisabled&&!t&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!_o(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let t=n.changedTouches;if(t)for(let i=0;i<t.length;i++)this.fadeInRipple(t[i].clientX,t[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let t=n.state===mt.VISIBLE||n.config.terminateOnPointerUp&&n.state===mt.FADING_IN;!n.config.persistent&&t&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(Av.forEach(t=>e._eventManager.removeHandler(t,n,this)),this._pointerUpEventsRegistered&&(Ov.forEach(t=>n.removeEventListener(t,this,Tv)),this._pointerUpEventsRegistered=!1))}};function EE(e,n,t){let i=Math.max(Math.abs(e-t.left),Math.abs(e-t.right)),r=Math.max(Math.abs(n-t.top),Math.abs(n-t.bottom));return Math.sqrt(i*i+r*r)}var xi=new y("mat-ripple-global-options"),Rv=(()=>{class e{_elementRef=m(B);_animationsDisabled=Ue();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(t){t&&this.fadeOutAllNonPersistent(),this._disabled=t,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(t){this._trigger=t,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let t=m(C),i=m(me),r=m(xi,{optional:!0}),o=m(ee);this._globalOptions=r||{},this._rippleRenderer=new wi(this,t,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:V(V(V({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(t,i=0,r){return typeof t=="number"?this._rippleRenderer.fadeInRipple(t,i,V(V({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,V(V({},this.rippleConfig),t))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return e})();var IE={capture:!0},ME=["focus","mousedown","mouseenter","touchstart"],$u="mat-ripple-loader-uninitialized",Wu="mat-ripple-loader-class-name",Nv="mat-ripple-loader-centered",za="mat-ripple-loader-disabled",Ha=(()=>{class e{_document=m(P);_animationsDisabled=Ue();_globalRippleOptions=m(xi,{optional:!0});_platform=m(me);_ngZone=m(C);_injector=m(ee);_eventCleanups;_hosts=new Map;constructor(){let t=m(Se).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>ME.map(i=>t.listen(this._document,i,this._onInteraction,IE)))}ngOnDestroy(){let t=this._hosts.keys();for(let i of t)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(t,i){t.setAttribute($u,this._globalRippleOptions?.namespace??""),(i.className||!t.hasAttribute(Wu))&&t.setAttribute(Wu,i.className||""),i.centered&&t.setAttribute(Nv,""),i.disabled&&t.setAttribute(za,"")}setDisabled(t,i){let r=this._hosts.get(t);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(t))):i?t.setAttribute(za,""):t.removeAttribute(za)}_onInteraction=t=>{let i=Qe(t);if(i instanceof HTMLElement){let r=i.closest(`[${$u}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(t){if(!this._document||this._hosts.has(t))return;t.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",t.getAttribute(Wu)),t.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??xo.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??xo.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||t.hasAttribute(za),rippleConfig:{centered:t.hasAttribute(Nv),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new wi(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(t),this._hosts.set(t,{target:a,renderer:c,hasSetUpEvents:l}),t.removeAttribute($u)}destroyRipple(t){let i=this._hosts.get(t);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(t))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var cn=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2})}return e})();var SE=["*",[["","progressIndicator",""]]],kE=["*","[progressIndicator]"];function TE(e,n){e&1&&(je(0,"div",1),x(1,1),ze())}var AE=new y("MAT_BUTTON_CONFIG");function Fv(e){return e==null?void 0:ao(e)}var Gu=(()=>{class e{_elementRef=m(B);_ngZone=m(C);_animationsDisabled=Ue();_config=m(AE,{optional:!0});_focusMonitor=m(_i);_cleanupClick;_renderer=m(Ke);_rippleLoader=m(Ha);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=t,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(t){this.tabIndex=t}showProgress=Ng(!1,{transform:le});constructor(){m(Le).load(cn);let t=this._elementRef.nativeElement;this._isAnchor=t.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(t,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(t="program",i){t?this._focusMonitor.focusVia(this._elementRef.nativeElement,t,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",t=>{this.disabled&&(t.preventDefault(),t.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(ie("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),jt(r.color?"mat-"+r.color:""),N("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",le],disabled:[2,"disabled","disabled",le],ariaDisabled:[2,"aria-disabled","ariaDisabled",le],disabledInteractive:[2,"disabledInteractive","disabledInteractive",le],tabIndex:[2,"tabIndex","tabIndex",Fv],_tabindex:[2,"tabindex","_tabindex",Fv],showProgress:[1,"showProgress"]}})}return e})(),qu=(()=>{class e extends Gu{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[_e],ngContentSelectors:kE,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(re(SE),lt(0,"span",0),x(1),Z(2,TE,2,0,"div",1),lt(3,"span",2)(4,"span",3)),i&2&&(z(2),Y(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return e})();var On=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J]})}return e})();var OE=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],RE=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function NE(e,n){e&1&&(je(0,"div",2),x(1,3),ze())}var Pv=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Lv=(()=>{class e extends Gu{get appearance(){return this._appearance}set appearance(t){this.setAppearance(t||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let t=FE(this._elementRef.nativeElement);t&&this.setAppearance(t)}setAppearance(t){if(t===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Pv.get(this._appearance):null,o=Pv.get(t);r&&i.remove(...r),i.add(...o),this._appearance=t}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[_e],ngContentSelectors:RE,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(re(OE),lt(0,"span",0),x(1),je(2,"span",1),x(3,1),ze(),x(4,2),Z(5,NE,2,0,"div",2),lt(6,"span",3)(7,"span",4)),i&2&&(N("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),z(5),Y(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --mat-progress-spinner-active-indicator-color: var(--mat-button-filled-progress-active-indicator-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return e})();function FE(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var Vv=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[On,J]})}return e})();function Bv(e){return Error(`Unable to find icon with the name "${e}"`)}function LE(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function jv(e){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${e}".`)}function zv(e){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${e}".`)}var ln=class{url;svgText;options;svgElement=null;constructor(n,t,i){this.url=n,this.svgText=t,this.options=i}},Uv=(()=>{class e{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(t,i,r,o){this._httpClient=t,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(t,i,r){return this.addSvgIconInNamespace("",t,i,r)}addSvgIconLiteral(t,i,r){return this.addSvgIconLiteralInNamespace("",t,i,r)}addSvgIconInNamespace(t,i,r,o){return this._addSvgIconConfig(t,i,new ln(r,null,o))}addSvgIconResolver(t){return this._resolvers.push(t),this}addSvgIconLiteralInNamespace(t,i,r,o){let s=this._sanitizer.sanitize(We.HTML,r);if(!s)throw zv(r);let a=gr(s);return this._addSvgIconConfig(t,i,new ln("",a,o))}addSvgIconSet(t,i){return this.addSvgIconSetInNamespace("",t,i)}addSvgIconSetLiteral(t,i){return this.addSvgIconSetLiteralInNamespace("",t,i)}addSvgIconSetInNamespace(t,i,r){return this._addSvgIconSetConfig(t,new ln(i,null,r))}addSvgIconSetLiteralInNamespace(t,i,r){let o=this._sanitizer.sanitize(We.HTML,i);if(!o)throw zv(i);let s=gr(o);return this._addSvgIconSetConfig(t,new ln("",s,r))}registerFontClassAlias(t,i=t){return this._fontCssClassesByAlias.set(t,i),this}classNameForFontAlias(t){return this._fontCssClassesByAlias.get(t)||t}setDefaultFontSetClass(...t){return this._defaultFontSetClass=t,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(t){let i=this._sanitizer.sanitize(We.RESOURCE_URL,t);if(!i)throw jv(t);let r=this._cachedIconsByUrl.get(i);return r?Xe(Ua(r)):this._loadSvgIconFromConfig(new ln(t,null)).pipe(Wn(o=>this._cachedIconsByUrl.set(i,o)),ae(o=>Ua(o)))}getNamedSvgIcon(t,i=""){let r=Hv(i,t),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,t),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(t,s):Mc(Bv(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(t){return t.svgText?Xe(Ua(this._svgElementFromConfig(t))):this._loadSvgIconFromConfig(t).pipe(ae(i=>Ua(i)))}_getSvgFromIconSetConfigs(t,i){let r=this._extractIconWithNameFromAnySet(t,i);if(r)return Xe(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(is(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(We.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),Xe(null)})));return Sc(o).pipe(ae(()=>{let s=this._extractIconWithNameFromAnySet(t,i);if(!s)throw Bv(t);return s}))}_extractIconWithNameFromAnySet(t,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(t)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,t,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(t){return this._fetchIcon(t).pipe(Wn(i=>t.svgText=i),ae(()=>this._svgElementFromConfig(t)))}_loadSvgIconSetFromConfig(t){return t.svgText?Xe(null):this._fetchIcon(t).pipe(Wn(i=>t.svgText=i))}_extractSvgIconFromSet(t,i,r){let o=t.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(gr("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(t){let i=this._document.createElement("DIV");i.innerHTML=t;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(t){let i=this._svgElementFromString(gr("<svg></svg>")),r=t.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<t.childNodes.length;o++)t.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(t.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(t,i){return t.setAttribute("fit",""),t.setAttribute("height","100%"),t.setAttribute("width","100%"),t.setAttribute("preserveAspectRatio","xMidYMid meet"),t.setAttribute("focusable","false"),i&&i.viewBox&&t.setAttribute("viewBox",i.viewBox),t}_fetchIcon(t){let{url:i,options:r}=t,o=r?.withCredentials??!1;if(!this._httpClient)throw LE();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(We.RESOURCE_URL,i);if(!s)throw jv(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ae(l=>gr(l)),Mr(()=>this._inProgressUrlFetches.delete(s)),Sr());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(t,i,r){return this._svgIconConfigs.set(Hv(t,i),r),this}_addSvgIconSetConfig(t,i){let r=this._iconSetConfigs.get(t);return r?r.push(i):this._iconSetConfigs.set(t,[i]),this}_svgElementFromConfig(t){if(!t.svgElement){let i=this._svgElementFromString(t.svgText);this._setSvgAttributes(i,t.options),t.svgElement=i}return t.svgElement}_getIconConfigFromResolvers(t,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,t);if(o)return VE(o)?new ln(o.url,null,o.options):new ln(o,null)}}static \u0275fac=function(i){return new(i||e)(T(Mu,8),T(Su),T(P,8),T(Ze))};static \u0275prov=X({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Ua(e){return e.cloneNode(!0)}function Hv(e,n){return e+":"+n}function VE(e){return!!(e.url&&e.options)}var BE=["*"],jE=new y("MAT_ICON_DEFAULT_OPTIONS"),zE=new y("mat-icon-location",{providedIn:"root",factory:()=>{let e=m(P),n=e?e.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),$v=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],HE=$v.map(e=>`[${e}]`).join(", "),UE=/^url\(['"]?#(.*?)['"]?\)$/,Wv=(()=>{class e{_elementRef=m(B);_iconRegistry=m(Uv);_location=m(zE);_errorHandler=m(Ze);_defaultColor;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(t){t!==this._svgIcon&&(t?this._updateSvgIcon(t):this._svgIcon&&this._clearSvgElement(),this._svgIcon=t)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(t){let i=this._cleanupFontValue(t);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(t){let i=this._cleanupFontValue(t);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=se.EMPTY;constructor(){let t=m(new va("aria-hidden"),{optional:!0}),i=m(jE,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),t||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(t){if(!t)return["",""];let i=t.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${t}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let t=this._elementsWithExternalReferences;if(t&&t.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(t){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(t),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(t)}_clearSvgElement(){let t=this._elementRef.nativeElement,i=t.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=t.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let t=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>t.classList.remove(r)),i.forEach(r=>t.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&t.classList.remove(this._previousFontIconClass),this.fontIcon&&t.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(t){return typeof t=="string"?t.trim().split(" ")[0]:t}_prependPathToReferences(t){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${t}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(t){let i=t.querySelectorAll(HE),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)$v.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(UE):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(t){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),t){let[i,r]=this._splitIconName(t);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Ht(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(ie("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),jt(r.color?"mat-"+r.color:""),N("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",le],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:BE,decls:1,vars:0,template:function(i,r){i&1&&(re(),x(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return e})(),Gv=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J]})}return e})();var qv=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J]})}return e})();var Zv=new y("");var Yv=(()=>{class e{isErrorState(t,i){return!!(t&&t.invalid&&(t.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var Zu=class{_box;_destroyed=new w;_resizeSubject=new w;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(t=>this._resizeSubject.next(t)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new Q(t=>{let i=this._resizeSubject.subscribe(t);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(ve(t=>t.some(i=>i.target===n)),ss({bufferSize:1,refCount:!0}),pe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},Kv=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=m(C);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,t]of this._observers)t.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(t,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Zu(r)),this._observers.get(r).observe(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var WE=["notch"],GE=["*"],Qv=["iconPrefixContainer"],Xv=["textPrefixContainer"],Jv=["iconSuffixContainer"],e_=["textSuffixContainer"],qE=["textField"],ZE=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],YE=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function KE(e,n){e&1&&W(0,"span",21)}function QE(e,n){if(e&1&&(u(0,"label",20),x(1,1),Z(2,KE,1,0,"span",21),f()),e&2){let t=he(2);Fe("floating",t._shouldLabelFloat())("monitorResize",t._hasOutline())("id",t._labelId),ie("for",t._control.disableAutomaticLabeling?null:t._control.id),z(2),Y(!t.hideRequiredMarker&&t._control.required?2:-1)}}function XE(e,n){if(e&1&&Z(0,QE,3,5,"label",20),e&2){let t=he();Y(t._hasFloatingLabel()?0:-1)}}function JE(e,n){e&1&&W(0,"div",7)}function eI(e,n){}function tI(e,n){if(e&1&&nn(0,eI,0,0,"ng-template",13),e&2){he(2);let t=Bt(1);Fe("ngTemplateOutlet",t)}}function nI(e,n){if(e&1&&(u(0,"div",9),Z(1,tI,1,1,null,13),f()),e&2){let t=he();Fe("matFormFieldNotchedOutlineOpen",t._shouldLabelFloat()),z(),Y(t._forceDisplayInfixLabel()?-1:1)}}function iI(e,n){e&1&&(u(0,"div",10,2),x(2,2),f())}function rI(e,n){e&1&&(u(0,"div",11,3),x(2,3),f())}function oI(e,n){}function sI(e,n){if(e&1&&nn(0,oI,0,0,"ng-template",13),e&2){he();let t=Bt(1);Fe("ngTemplateOutlet",t)}}function aI(e,n){e&1&&(u(0,"div",14,4),x(2,4),f())}function cI(e,n){e&1&&(u(0,"div",15,5),x(2,5),f())}function lI(e,n){e&1&&W(0,"div",16)}function dI(e,n){e&1&&(u(0,"div",18),x(1,6),f())}function uI(e,n){if(e&1&&(u(0,"mat-hint",22),p(1),f()),e&2){let t=he(2);Fe("id",t._hintLabelId),z(),no(t.hintLabel)}}function fI(e,n){if(e&1&&(u(0,"div",19),Z(1,uI,2,2,"mat-hint",22),x(2,7),W(3,"div",23),x(4,8),f()),e&2){let t=he();z(),Y(t.hintLabel?1:-1)}}var Yu=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["mat-label"]]})}return e})(),mI=new y("MatError");var Ku=(()=>{class e{align="start";id=m(ft).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Dt("id",r.id),ie("align",null),N("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),pI=new y("MatPrefix");var hI=new y("MatSuffix");var a_=new y("FloatingLabelParent"),t_=(()=>{class e{_elementRef=m(B);get floating(){return this._floating}set floating(t){this._floating=t,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(t){this._monitorResize=t,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=m(Kv);_ngZone=m(C);_parent=m(a_);_resizeSubscription=new se;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return gI(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&N("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function gI(e){let n=e;if(n.offsetParent!==null)return n.scrollWidth;let t=n.cloneNode(!0);t.style.setProperty("position","absolute"),t.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(t);let i=t.scrollWidth;return t.remove(),i}var n_="mdc-line-ripple--active",$a="mdc-line-ripple--deactivating",i_=(()=>{class e{_elementRef=m(B);_cleanupTransitionEnd;constructor(){let t=m(C),i=m(Ke);t.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let t=this._elementRef.nativeElement.classList;t.remove($a),t.add(n_)}deactivate(){this._elementRef.nativeElement.classList.add($a)}_handleTransitionEnd=t=>{let i=this._elementRef.nativeElement.classList,r=i.contains($a);t.propertyName==="opacity"&&r&&i.remove(n_,$a)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),r_=(()=>{class e{_elementRef=m(B);_ngZone=m(C);open=!1;_notch;ngAfterViewInit(){let t=this._elementRef.nativeElement,i=t.querySelector(".mdc-floating-label");i?(t.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):t.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(t){let i=this._notch.nativeElement;!this.open||!t?i.style.width="":i.style.width=`calc(${t}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(t){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${t}px)`)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Pe(WE,5),i&2){let o;S(o=k())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&N("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:GE,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(re(),lt(0,"div",1),je(1,"div",2,0),x(3),ze(),lt(4,"div",3))},encapsulation:2})}return e})(),vI=(()=>{class e{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e})}return e})();var _I=new y("MatFormField"),yI=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),o_="fill",bI="auto",s_="fixed",wI="translateY(-50%)",c_=(()=>{class e{_elementRef=m(B);_changeDetectorRef=m(Ge);_platform=m(me);_idGenerator=m(ft);_ngZone=m(C);_defaults=m(yI,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=so("iconPrefixContainer");_textPrefixContainerSignal=so("textPrefixContainer");_iconSuffixContainerSignal=so("iconSuffixContainer");_textSuffixContainerSignal=so("textSuffixContainer");_prefixSuffixContainers=dr(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(t=>t?.nativeElement).filter(t=>t!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Fg(Yu);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(t){this._hideRequiredMarker=Te(t)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||bI}set floatLabel(t){t!==this._floatLabel&&(this._floatLabel=t,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(t){let i=t||this._defaults?.appearance||o_;this._appearanceSignal.set(i)}_appearanceSignal=Ee(o_);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||s_}set subscriptSizing(t){this._subscriptSizing=t||this._defaults?.subscriptSizing||s_}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(t){this._hintLabel=t,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(t){this._explicitFormFieldControl=t}_destroyed=new w;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ue();constructor(){let t=this._defaults,i=m(dt);t&&(t.appearance&&(this.appearance=t.appearance),this._hideRequiredMarker=!!t?.hideRequiredMarker,t.color&&(this.color=t.color)),ci(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=dr(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(t){let i=this._control,r="mat-mdc-form-field-type-";t&&this._elementRef.nativeElement.classList.remove(r+t.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(nt([void 0,void 0]),ae(()=>[i.errorState,i.userAriaDescribedBy]),os(),ve(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(pe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(t=>!t._isText),this._hasTextPrefix=!!this._prefixChildren.find(t=>t._isText),this._hasIconSuffix=!!this._suffixChildren.find(t=>!t._isText),this._hasTextSuffix=!!this._suffixChildren.find(t=>t._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Mt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let t=this._control.focused;t&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!t&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",t),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",t)}_syncOutlineLabelOffset(){Vg({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let t of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(t,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:t=>this._writeOutlinedLabelStyles(t())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=dr(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(t){let i=this._control?this._control.ngControl:null;return i&&i[t]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let t=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&t.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?t.push(o.id):this._hintLabel&&t.push(this._hintLabelId),s&&t.push(s.id)}else this._errorChildren&&t.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||t;r=t.concat(i.filter(s=>s&&!o.includes(s)))}else r=t;this._control.setDescribedByIds(r),this._describedByIds=t}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let t=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=t?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",h=`${s+a}px`,g=`calc(${d} * (${h} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,_=`var(--mat-mdc-form-field-label-transform, ${wI} translateX(${g}))`,b=s+a+c+l;return[_,b]}_writeOutlinedLabelStyles(t){if(t!==null){let[i,r]=t;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let t=this._elementRef.nativeElement;if(t.getRootNode){let i=t.getRootNode();return i&&i!==t}return document.documentElement.contains(t)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(fa(o,r._labelChild,Yu,5),He(o,vI,5)(o,pI,5)(o,hI,5)(o,mI,5)(o,Ku,5)),i&2){pa();let s;S(s=k())&&(r._formFieldControl=s.first),S(s=k())&&(r._prefixChildren=s),S(s=k())&&(r._suffixChildren=s),S(s=k())&&(r._errorChildren=s),S(s=k())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(ma(r._iconPrefixContainerSignal,Qv,5)(r._textPrefixContainerSignal,Xv,5)(r._iconSuffixContainerSignal,Jv,5)(r._textSuffixContainerSignal,e_,5),Pe(qE,5)(Qv,5)(Xv,5)(Jv,5)(e_,5)(t_,5)(r_,5)(i_,5)),i&2){pa(4);let o;S(o=k())&&(r._textField=o.first),S(o=k())&&(r._iconPrefixContainer=o.first),S(o=k())&&(r._textPrefixContainer=o.first),S(o=k())&&(r._iconSuffixContainer=o.first),S(o=k())&&(r._textSuffixContainer=o.first),S(o=k())&&(r._floatingLabel=o.first),S(o=k())&&(r._notchedOutline=o.first),S(o=k())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&N("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Ce([{provide:_I,useExisting:e},{provide:a_,useExisting:e}])],ngContentSelectors:YE,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(re(ZE),nn(0,XE,1,1,"ng-template",null,0,ro),u(2,"div",6,1),ue("click",function(s){return r._control.onContainerClick(s)}),Z(4,JE,1,0,"div",7),u(5,"div",8),Z(6,nI,2,2,"div",9),Z(7,iI,3,0,"div",10),Z(8,rI,3,0,"div",11),u(9,"div",12),Z(10,sI,1,1,null,13),x(11),f(),Z(12,aI,3,0,"div",14),Z(13,cI,3,0,"div",15),f(),Z(14,lI,1,0,"div",16),f(),u(15,"div",17),Z(16,dI,2,0,"div",18)(17,fI,5,1,"div",19),f()),i&2){let o;z(2),N("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),z(2),Y(!r._hasOutline()&&!r._control.disabled?4:-1),z(2),Y(r._hasOutline()?6:-1),z(),Y(r._hasIconPrefix?7:-1),z(),Y(r._hasTextPrefix?8:-1),z(2),Y(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),z(2),Y(r._hasTextSuffix?12:-1),z(),Y(r._hasIconSuffix?13:-1),z(),Y(r._hasOutline()?-1:14),z(),N("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();z(),Y((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[t_,r_,lu,i_,Ku],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return e})();var p_=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],h_=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function xI(e,n){e&1&&(u(0,"span",3),x(1,1),f())}function CI(e,n){e&1&&(u(0,"span",6),x(1,2),f())}function DI(e,n){e&1&&(u(0,"span",3),x(1,1),u(2,"span",7),vt(),u(3,"svg",8),W(4,"path",9),f()()())}function EI(e,n){e&1&&(u(0,"span",6),x(1,2),f())}var II=`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`;var g_=["*"],MI=`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`,Ju=new y("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),l_=new y("MatChipAvatar"),d_=new y("MatChipTrailingIcon"),u_=new y("MatChipEdit"),f_=new y("MatChipRemove"),ef=new y("MatChip"),v_=(()=>{class e{_elementRef=m(B);_parentChip=m(ef);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(t){this._disabled=t}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){m(Le).load(cn),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(ie("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),N("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",le],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?-1:ao(t)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return e})(),__=(()=>{class e extends v_{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(t){!this.disabled&&this._isPrimary&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275dir=$({type:e,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&ue("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(ie("tabindex",r._getTabindex()),N("mdc-evolution-chip__action--presentational",!1))},features:[_e]})}return e})();var Qu=(()=>{class e{_changeDetectorRef=m(Ge);_elementRef=m(B);_tagName=m(Rg);_ngZone=m(C);_focusMonitor=m(_i);_globalRippleOptions=m(xi,{optional:!0});_document=m(P);_onFocus=new w;_onBlur=new w;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Ue();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=m(ft).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(t){this._value=t}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(t){this._disabled=t}_disabled=!1;removed=new ce;destroyed=new ce;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=m(Ha);_injector=m(ee);constructor(){let t=m(Le);t.load(cn),t.load(hr),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=Mt(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(t){(t.keyCode===8&&!t.repeat||t.keyCode===46)&&(t.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(t){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===t||r.contains(t)})}_getActions(){let t=[];return this.editIcon&&t.push(this.editIcon),this.primaryAction&&t.push(this.primaryAction),this.removeIcon&&t.push(this.removeIcon),t}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(t){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{let i=t!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&He(o,l_,5)(o,u_,5)(o,d_,5)(o,f_,5)(o,l_,5)(o,d_,5)(o,u_,5)(o,f_,5),i&2){let s;S(s=k())&&(r.leadingIcon=s.first),S(s=k())&&(r.editIcon=s.first),S(s=k())&&(r.trailingIcon=s.first),S(s=k())&&(r.removeIcon=s.first),S(s=k())&&(r._allLeadingIcons=s),S(s=k())&&(r._allTrailingIcons=s),S(s=k())&&(r._allEditIcons=s),S(s=k())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&Pe(__,5),i&2){let o;S(o=k())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&ue("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Dt("id",r.id),ie("role",r.role)("aria-label",r.ariaLabel),jt("mat-"+(r.color||"primary")),N("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",le],highlighted:[2,"highlighted","highlighted",le],disableRipple:[2,"disableRipple","disableRipple",le],disabled:[2,"disabled","disabled",le]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[Ce([{provide:ef,useExisting:e}])],ngContentSelectors:h_,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(re(p_),W(0,"span",0),u(1,"span",1)(2,"span",2),Z(3,xI,2,0,"span",3),u(4,"span",4),x(5),W(6,"span",5),f()()(),Z(7,CI,2,0,"span",6)),i&2&&(z(3),Y(r.leadingIcon?3:-1),z(4),Y(r._hasTrailingIcon()?7:-1))},dependencies:[v_],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})}return e})();var tf=(()=>{class e extends Qu{_defaultOptions=m(Ju,{optional:!0});chipListSelectable=!0;_chipListMultiple=!1;_chipListHideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get selectable(){return this._selectable&&this.chipListSelectable}set selectable(t){this._selectable=t,this._changeDetectorRef.markForCheck()}_selectable=!0;get selected(){return this._selected}set selected(t){this._setSelectedState(t,!1,!0)}_selected=!1;get ariaSelected(){return this.selectable?this.selected.toString():null}basicChipAttrName="mat-basic-chip-option";selectionChange=new ce;ngOnInit(){super.ngOnInit(),this.role="presentation"}select(){this._setSelectedState(!0,!1,!0)}deselect(){this._setSelectedState(!1,!1,!0)}selectViaInteraction(){this._setSelectedState(!0,!0,!0)}toggleSelected(t=!1){return this._setSelectedState(!this.selected,t,!0),this.selected}_handlePrimaryActionInteraction(){this.disabled||(this.focus(),this.selectable&&this.toggleSelected(!0))}_hasLeadingGraphic(){return this.leadingIcon?!0:!this._chipListHideSingleSelectionIndicator||this._chipListMultiple}_setSelectedState(t,i,r){t!==this.selected&&(this._selected=t,r&&this.selectionChange.emit({source:this,isUserInput:i,selected:this.selected}),this._changeDetectorRef.markForCheck())}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-basic-chip-option"],["","mat-basic-chip-option",""],["mat-chip-option"],["","mat-chip-option",""]],hostAttrs:[1,"mat-mdc-chip","mat-mdc-chip-option"],hostVars:37,hostBindings:function(i,r){i&2&&(Dt("id",r.id),ie("tabindex",null)("aria-label",null)("aria-description",null)("role",r.role),N("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--filter",!r._isBasicChip)("mdc-evolution-chip--selectable",!r._isBasicChip)("mat-mdc-chip-selected",r.selected)("mat-mdc-chip-multiple",r._chipListMultiple)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-chip-with-avatar",r.leadingIcon)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--selected",r.selected)("mdc-evolution-chip--selecting",!r._animationsDisabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-primary-graphic",r._hasLeadingGraphic())("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon()))},inputs:{selectable:[2,"selectable","selectable",le],selected:[2,"selected","selected",le]},outputs:{selectionChange:"selectionChange"},features:[Ce([{provide:Qu,useExisting:e},{provide:ef,useExisting:e}]),_e],ngContentSelectors:h_,decls:8,vars:6,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipAction","","role","option",3,"_allowFocusWhenDisabled"],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"],[1,"mdc-evolution-chip__checkmark"],["viewBox","-2 -3 30 30","focusable","false","aria-hidden","true",1,"mdc-evolution-chip__checkmark-svg"],["fill","none","stroke","currentColor","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-evolution-chip__checkmark-path"]],template:function(i,r){i&1&&(re(p_),W(0,"span",0),u(1,"span",1)(2,"button",2),Z(3,DI,5,0,"span",3),u(4,"span",4),x(5),W(6,"span",5),f()()(),Z(7,EI,2,0,"span",6)),i&2&&(z(2),Fe("_allowFocusWhenDisabled",!0),ie("aria-description",r.ariaDescription)("aria-label",r.ariaLabel)("aria-selected",r.ariaSelected),z(),Y(r._hasLeadingGraphic()?3:-1),z(4),Y(r._hasTrailingIcon()?7:-1))},dependencies:[__],styles:[II],encapsulation:2})}return e})();var SI=(()=>{class e{_elementRef=m(B);_changeDetectorRef=m(Ge);_dir=m(dt,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new w;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(t=>t._onFocus)}get chipDestroyedChanges(){return this._getChipStream(t=>t.destroyed)}get chipRemovedChanges(){return this._getChipStream(t=>t.removed)}get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(t){this._explicitRole=t}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new bt;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(t=>t._hasFocus())}_syncChipsState(){this._chips?.forEach(t=>{t._chipListDisabled=this._disabled,t._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(t){this._originatesFromChip(t)&&this._keyManager.onKeydown(t)}_isValidIndex(t){return t>=0&&t<this._chips.length}_allowFocusEscape(){let t=this._elementRef.nativeElement.tabIndex;t!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=t))}_getChipStream(t){return this._chips.changes.pipe(nt(null),kr(()=>Mt(...this._chips.map(t))))}_originatesFromChip(t){let i=t.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(nt(this._chips)).subscribe(t=>{let i=[];t.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new bo(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(t=>this._skipPredicate(t)),this.chipFocusChanges.pipe(pe(this._destroyed)).subscribe(({chip:t})=>{let i=t._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(pe(this._destroyed)).subscribe(t=>this._keyManager.withHorizontalOrientation(t))}_skipPredicate(t){return t.disabled}_trackChipSetChanges(){this._chips.changes.pipe(nt(null),pe(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(pe(this._destroyed)).subscribe(t=>{let r=this._chips.toArray().indexOf(t.chip),o=t.chip._hasFocus(),s=t.chip._hadFocusOnRemove&&this._keyManager.activeItem&&t.chip._getActions().includes(this._keyManager.activeItem),a=o||s;this._isValidIndex(r)&&a&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let t=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[t];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&He(o,Qu,5),i&2){let s;S(s=k())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&ue("keydown",function(s){return r._handleKeydown(s)}),i&2&&ie("role",r.role)},inputs:{disabled:[2,"disabled","disabled",le],role:"role",tabIndex:[2,"tabIndex","tabIndex",t=>t==null?0:ao(t)]},ngContentSelectors:g_,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(re(),je(0,"div",0),x(1),ze())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})}return e})(),Xu=class{source;value;constructor(n,t){this.source=n,this.value=t}},kI={provide:Zv,useExisting:Wt(()=>nf),multi:!0},nf=(()=>{class e extends SI{_onTouched=()=>{};_onChange=()=>{};_defaultRole="listbox";_defaultOptions=m(Ju,{optional:!0});get multiple(){return this._multiple}set multiple(t){this._multiple=t,this._syncListboxProperties()}_multiple=!1;get selected(){let t=this._chips.toArray().filter(i=>i.selected);return this.multiple?t:t[0]}ariaOrientation="horizontal";get selectable(){return this._selectable}set selectable(t){this._selectable=t,this._syncListboxProperties()}_selectable=!0;compareWith=(t,i)=>t===i;required=!1;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(t){this._hideSingleSelectionIndicator=t,this._syncListboxProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get chipSelectionChanges(){return this._getChipStream(t=>t.selectionChange)}get chipBlurChanges(){return this._getChipStream(t=>t._onBlur)}get value(){return this._value}set value(t){this._chips&&this._chips.length&&this._setSelectionByValue(t,!1),this._value=t}_value;change=new ce;_chips=void 0;ngAfterContentInit(){this._chips.changes.pipe(nt(null),pe(this._destroyed)).subscribe(()=>{this.value!==void 0&&Promise.resolve().then(()=>{this._setSelectionByValue(this.value,!1)}),this._syncListboxProperties()}),this.chipBlurChanges.pipe(pe(this._destroyed)).subscribe(()=>this._blur()),this.chipSelectionChanges.pipe(pe(this._destroyed)).subscribe(t=>{this.multiple||this._chips.forEach(i=>{i!==t.source&&i._setSelectedState(!1,!1,!1)}),t.isUserInput&&this._propagateChanges()})}focus(){if(this.disabled)return;let t=this._getFirstSelectedChip();t&&!t.disabled?t.focus():this._chips.length>0?this._keyManager.setFirstItemActive():this._elementRef.nativeElement.focus()}writeValue(t){t!=null?this.value=t:this.value=void 0}registerOnChange(t){this._onChange=t}registerOnTouched(t){this._onTouched=t}setDisabledState(t){this.disabled=t}_setSelectionByValue(t,i=!0){this._clearSelection(),Array.isArray(t)?t.forEach(r=>this._selectValue(r,i)):this._selectValue(t,i)}_blur(){this.disabled||setTimeout(()=>{this.focused||this._markAsTouched()})}_keydown(t){t.keyCode===9&&super._allowFocusEscape()}_markAsTouched(){this._onTouched(),this._changeDetectorRef.markForCheck()}_propagateChanges(){let t=null;Array.isArray(this.selected)?t=this.selected.map(i=>i.value):t=this.selected?this.selected.value:void 0,this._value=t,this.change.emit(new Xu(this,t)),this._onChange(t),this._changeDetectorRef.markForCheck()}_clearSelection(t){this._chips.forEach(i=>{i!==t&&i.deselect()})}_selectValue(t,i){let r=this._chips.find(o=>o.value!=null&&this.compareWith(o.value,t));return r&&(i?r.selectViaInteraction():r.select()),r}_syncListboxProperties(){this._chips&&Promise.resolve().then(()=>{this._chips.forEach(t=>{t._chipListMultiple=this.multiple,t.chipListSelectable=this._selectable,t._chipListHideSingleSelectionIndicator=this.hideSingleSelectionIndicator,t._changeDetectorRef.markForCheck()})})}_getFirstSelectedChip(){return Array.isArray(this.selected)?this.selected.length?this.selected[0]:void 0:this.selected}_skipPredicate(t){return!1}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-chip-listbox"]],contentQueries:function(i,r,o){if(i&1&&He(o,tf,5),i&2){let s;S(s=k())&&(r._chips=s)}},hostAttrs:[1,"mdc-evolution-chip-set","mat-mdc-chip-listbox"],hostVars:10,hostBindings:function(i,r){i&1&&ue("focus",function(){return r.focus()})("blur",function(){return r._blur()})("keydown",function(s){return r._keydown(s)}),i&2&&(Dt("tabIndex",r.disabled||r.empty?-1:r.tabIndex),ie("role",r.role)("aria-required",r.role?r.required:null)("aria-disabled",r.disabled.toString())("aria-multiselectable",r.multiple)("aria-orientation",r.ariaOrientation),N("mat-mdc-chip-list-disabled",r.disabled)("mat-mdc-chip-list-required",r.required))},inputs:{multiple:[2,"multiple","multiple",le],ariaOrientation:[0,"aria-orientation","ariaOrientation"],selectable:[2,"selectable","selectable",le],compareWith:"compareWith",required:[2,"required","required",le],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",le],value:"value"},outputs:{change:"change"},features:[Ce([kI]),_e],ngContentSelectors:g_,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(re(),je(0,"div",0),x(1),ze())},styles:[MI],encapsulation:2})}return e})();var y_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({providers:[Yv,{provide:Ju,useValue:{separatorKeyCodes:[13]}}],imports:[On,J]})}return e})();var Ci=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[Ra,c_,J]})}return e})();var b_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({})}return e})();var w_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[Ci,Ci,b_,J]})}return e})();var Co=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},rf=class extends Co{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,t,i,r,o,s){super(),this.component=n,this.viewContainerRef=t,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Ga=class extends Co{templateRef;viewContainerRef;context;injector;constructor(n,t,i,r){super(),this.templateRef=n,this.viewContainerRef=t,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,t=this.context){return this.context=t,super.attach(n)}detach(){return this.context=void 0,super.detach()}},of=class extends Co{element;constructor(n){super(),this.element=n instanceof B?n.nativeElement:n}},sf=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof rf)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Ga)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof of)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},qa=class extends sf{outletElement;_appRef;_defaultInjector;constructor(n,t,i){super(),this.outletElement=n,this._appRef=t,this._defaultInjector=i}attachComponentPortal(n){let t;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(ui,null,{optional:!0})||void 0;t=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>t.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||ee.NULL,o=r.get(be,i.injector);t=_a(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(t.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(t.hostView),t.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(t)),this._attachedPortal=n,t}attachTemplatePortal(n){let t=n.viewContainerRef,i=t.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=t.indexOf(i);r!==-1&&t.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let t=n.element;t.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");t.parentNode.insertBefore(i,t),this.outletElement.appendChild(t),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(t,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var x_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({})}return e})();var C_=ka();function T_(e){return new Za(e.get(An),e.get(P))}var Za=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,t){this._viewportRuler=n,this._document=t}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=we(-this._previousScrollPosition.left),n.style.top=we(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,t=this._document.body,i=n.style,r=t.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),C_&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),C_&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let t=this._document.documentElement,i=this._viewportRuler.getViewportSize();return t.scrollHeight>i.height||t.scrollWidth>i.width}};function A_(e,n){return new Ya(e.get(Va),e.get(C),e.get(An),n)}var Ya=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,t,i,r){this._scrollDispatcher=n,this._ngZone=t,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(ve(t=>!t||!this._overlayRef.overlayElement.contains(t.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let t=this._viewportRuler.getViewportScrollPosition().top;Math.abs(t-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Do=class{enable(){}disable(){}attach(){}};function af(e,n){return n.some(t=>{let i=e.bottom<t.top,r=e.top>t.bottom,o=e.right<t.left,s=e.left>t.right;return i||r||o||s})}function D_(e,n){return n.some(t=>{let i=e.top<t.top,r=e.bottom>t.bottom,o=e.left<t.left,s=e.right>t.right;return i||r||o||s})}function lf(e,n){return new Ka(e.get(Va),e.get(An),e.get(C),n)}var Ka=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,t,i,r){this._scrollDispatcher=n,this._viewportRuler=t,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let t=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();af(t,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},O_=(()=>{class e{_injector=m(ee);noop=()=>new Do;close=t=>A_(this._injector,t);block=()=>T_(this._injector);reposition=t=>lf(this._injector,t);static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),Qa=class{positionStrategy;scrollStrategy=new Do;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let t=Object.keys(n);for(let i of t)n[i]!==void 0&&(this[i]=n[i])}}};var Xa=class{connectionPair;scrollableViewProperties;constructor(n,t){this.connectionPair=n,this.scrollableViewProperties=t}};var R_=(()=>{class e{_attachedOverlays=[];_document=m(P);_isAttached=!1;ngOnDestroy(){this.detach()}add(t){this.remove(t),this._attachedOverlays.push(t)}remove(t){let i=this._attachedOverlays.indexOf(t);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(t,i,r){return r.observers.length<1?!1:t.eventPredicate?t.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),N_=(()=>{class e extends R_{_ngZone=m(C);_renderer=m(Se).createRenderer(null,null);_cleanupKeydown;add(t){super.add(t),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=t=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,t,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(t));break}}};static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),F_=(()=>{class e extends R_{_platform=m(me);_ngZone=m(C);_renderer=m(Se).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(t){if(super.add(t),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(t=>t()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=t=>{this._pointerDownEventTarget=Qe(t)};_clickListener=t=>{let i=Qe(t),r=t.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,t,c))){if(E_(a.overlayElement,i)||E_(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(t)):c.next(t)}}};static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();function E_(e,n){let t=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===e)return!0;i=t&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var P_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return e})(),L_=(()=>{class e{_platform=m(me);_containerElement;_document=m(P);_styleLoader=m(Le);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let t="cdk-overlay-container";if(this._platform.isBrowser||Ou()){let r=this._document.querySelectorAll(`.${t}[platform="server"], .${t}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(t),Ou()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(P_)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),cf=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,t,i,r){this._renderer=t,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=t.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function df(e){return e&&e.nodeType===1}var Ja=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new w;_attachments=new w;_detachments=new w;_positionStrategy;_scrollStrategy;_locationChanges=se.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new w;_outsidePointerEvents=new w;_afterNextRenderRef;constructor(n,t,i,r,o,s,a,c,l,d=!1,h,v){this._portalOutlet=n,this._host=t,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=h,this._renderer=v,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let t=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=ct(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof t?.onDestroy=="function"&&t.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),t}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=V(V({},this._config),n),this._updateElementSize()}setDirection(n){this._config=ye(V({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=we(this._config.width),n.height=we(this._config.height),n.minWidth=we(this._config.minWidth),n.minHeight=we(this._config.minHeight),n.maxWidth=we(this._config.maxWidth),n.maxHeight=we(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;df(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new cf(this._document,this._renderer,this._ngZone,t=>{this._backdropClick.next(t)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,t,i){let r=Nu(t||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=ct(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(t){if(n)throw t;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},I_="cdk-overlay-connected-position-bounding-box",AI=/([A-Za-z%]+)$/;function V_(e,n){return new ec(n,e.get(An),e.get(P),e.get(me),e.get(L_))}var ec=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new w;_resizeSubscription=se.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,t,i,r,o){this._viewportRuler=t,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(I_),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,t=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,t,a),d=this._getOverlayFit(l,t,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:t,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:t})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Di(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(I_),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof B?this._origin.nativeElement:df(this._origin)?this._origin:null}_getOriginPoint(n,t,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}t.left<0&&(r-=t.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,t.top<0&&(o-=t.top),{x:r,y:o}}_getOverlayPoint(n,t,i){let r;i.overlayX=="center"?r=-t.width/2:i.overlayX==="start"?r=this._isRtl()?-t.width:0:r=this._isRtl()?0:-t.width;let o;return i.overlayY=="center"?o=-t.height/2:o=i.overlayY=="top"?0:-t.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,t,i,r){let o=S_(t),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,h=s+o.width-i.width,v=0-a,g=a+o.height-i.height,_=this._subtractOverflows(o.width,d,h),b=this._subtractOverflows(o.height,v,g),I=_*b;return{visibleArea:I,isCompletelyWithinViewport:o.width*o.height===I,fitsInViewportVertically:b===o.height,fitsInViewportHorizontally:_==o.width}}_canFitWithFlexibleDimensions(n,t,i){if(this._hasFlexibleDimensions){let r=i.bottom-t.y,o=i.right-t.x,s=M_(this._overlayRef.getConfig().minHeight),a=M_(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,t,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=S_(t),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,h=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?h=c||-a:h=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:h},{x:n.x+d,y:n.y+h}}_applyPosition(n,t){if(this._setTransformOrigin(n),this._setOverlayElementStyles(t,n),this._setBoundingBoxStyles(t,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!OI(this._lastScrollVisibility,i)){let r=new Xa(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let t=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<t.length;o++)t[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,t){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(t.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(t.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let g=Math.min(i.bottom-n.y+i.top,n.y),_=this._lastBoundingBoxSize.height;o=g*2,s=n.y-g,o>_&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-_/2)}let c=t.overlayX==="start"&&!r||t.overlayX==="end"&&r,l=t.overlayX==="end"&&!r||t.overlayX==="start"&&r,d,h,v;if(l)v=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)h=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let g=Math.min(i.right-n.x+i.left,n.x),_=this._lastBoundingBoxSize.width;d=g*2,h=n.x-g,d>_&&!this._isInitialRender&&!this._growAfterOpen&&(h=n.x-_/2)}return{top:s,left:h,bottom:a,right:v,width:d,height:o}}_setBoundingBoxStyles(n,t){let i=this._calculateBoundingBoxRect(n,t);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=we(i.width),r.height=we(i.height),r.top=we(i.top)||"auto",r.bottom=we(i.bottom)||"auto",r.left=we(i.left)||"auto",r.right=we(i.right)||"auto",t.overlayX==="center"?r.alignItems="center":r.alignItems=t.overlayX==="end"?"flex-end":"flex-start",t.overlayY==="center"?r.justifyContent="center":r.justifyContent=t.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=we(o)),s&&(r.maxWidth=we(s))}this._lastBoundingBoxSize=i,Di(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Di(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Di(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,t){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();Di(i,this._getExactOverlayY(t,n,d)),Di(i,this._getExactOverlayX(t,n,d))}else i.position="static";let a="",c=this._getOffset(t,"x"),l=this._getOffset(t,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=we(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=we(s.maxWidth):o&&(i.maxWidth="")),Di(this._pane.style,i)}_getExactOverlayY(n,t,i){let r={top:"",bottom:""},o=this._getOverlayPoint(t,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=we(o.y);return r}_getExactOverlayX(n,t,i){let r={left:"",right:""},o=this._getOverlayPoint(t,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=we(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),t=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:D_(n,i),isOriginOutsideView:af(n,i),isOverlayClipped:D_(t,i),isOverlayOutsideView:af(t,i)}}_subtractOverflows(n,...t){return t.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,t=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+t-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:t-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,t){return t==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Nu(n).forEach(t=>{t!==""&&this._appliedPanelClasses.indexOf(t)===-1&&(this._appliedPanelClasses.push(t),this._pane.classList.add(t))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof B)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let t=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+t,height:i,width:t}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",t=this._overlayContainer.getContainerElement();n&&(t.style.display="block");let i=t.getBoundingClientRect();return n&&(t.style.display=""),i}};function Di(e,n){for(let t in n)n.hasOwnProperty(t)&&(e[t]=n[t]);return e}function M_(e){if(typeof e!="number"&&e!=null){let[n,t]=e.split(AI);return!t||t==="px"?parseFloat(n):null}return e||null}function S_(e){return{top:Math.floor(e.top),right:Math.floor(e.right),bottom:Math.floor(e.bottom),left:Math.floor(e.left),width:Math.floor(e.width),height:Math.floor(e.height)}}function OI(e,n){return e===n?!0:e.isOriginClipped===n.isOriginClipped&&e.isOriginOutsideView===n.isOriginOutsideView&&e.isOverlayClipped===n.isOverlayClipped&&e.isOverlayOutsideView===n.isOverlayOutsideView}var k_="cdk-global-overlay-wrapper";function B_(e){return new tc}var tc=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let t=n.getConfig();this._overlayRef=n,this._width&&!t.width&&n.updateSize({width:this._width}),this._height&&!t.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(k_),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,h=this._xOffset,v=this._overlayRef.getConfig().direction==="rtl",g="",_="",b="";c?b="flex-start":d==="center"?(b="center",v?_=h:g=h):v?d==="left"||d==="end"?(b="flex-end",g=h):(d==="right"||d==="start")&&(b="flex-start",_=h):d==="left"||d==="start"?(b="flex-start",g=h):(d==="right"||d==="end")&&(b="flex-end",_=h),n.position=this._cssPosition,n.marginLeft=c?"0":g,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":_,t.justifyContent=b,t.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement,i=t.style;t.classList.remove(k_),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},j_=(()=>{class e{_injector=m(ee);global(){return B_()}flexibleConnectedTo(t){return V_(this._injector,t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})(),uf=new y("OVERLAY_DEFAULT_CONFIG");function z_(e,n){e.get(Le).load(P_);let t=e.get(L_),i=e.get(P),r=e.get(ft),o=e.get(Ct),s=e.get(dt),a=e.get(Ke,null,{optional:!0})||e.get(Se).createRenderer(null,null),c=new Qa(n),l=e.get(uf,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),h=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),h.appendChild(d),c.usePopover&&(h.setAttribute("popover","manual"),h.classList.add("cdk-overlay-popover"));let v=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return df(v)?v.after(h):v?.type==="parent"?v.element.appendChild(h):t.getContainerElement().appendChild(h),new Ja(new qa(d,o,e),h,d,c,e.get(C),e.get(N_),i,e.get(wa),e.get(F_),n?.disableAnimations??e.get(Hr,null,{optional:!0})==="NoopAnimations",e.get(be),a)}var H_=(()=>{class e{scrollStrategies=m(O_);_positionBuilder=m(j_);_injector=m(ee);create(t){return z_(this._injector,t)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||e)};static \u0275prov=L({token:e,factory:e.\u0275fac})}return e})();var ff=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({providers:[H_],imports:[J,x_,Lu,Lu]})}return e})();var U_=(()=>{class e{_animationsDisabled=Ue();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&N("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return e})();var FI=["text"],PI=[[["mat-icon"]],"*"],LI=["mat-icon","*"];function VI(e,n){if(e&1&&W(0,"mat-pseudo-checkbox",1),e&2){let t=he();Fe("disabled",t.disabled)("state",t.selected?"checked":"unchecked")}}function BI(e,n){if(e&1&&W(0,"mat-pseudo-checkbox",3),e&2){let t=he();Fe("disabled",t.disabled)}}function jI(e,n){if(e&1&&(u(0,"span",4),p(1),f()),e&2){let t=he();z(),io("(",t.group.label,")")}}var zI=new y("MAT_OPTION_PARENT_COMPONENT"),HI=new y("MatOptgroup");var mf=class{source;isUserInput;constructor(n,t=!1){this.source=n,this.isUserInput=t}},$_=(()=>{class e{_element=m(B);_changeDetectorRef=m(Ge);_parent=m(zI,{optional:!0});group=m(HI,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=m(ft).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(t){this._disabled.set(t)}_disabled=Ee(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new ce;_text;_stateChanges=new w;constructor(){let t=m(Le);t.load(cn),t.load(hr),this._signalDisableRipple=!!this._parent&&En(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(t=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),t&&this._emitSelectionChangeEvent())}deselect(t=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),t&&this._emitSelectionChangeEvent())}focus(t,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!vr(t)&&(this._selectViaInteraction(),t.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let t=this.viewValue;t!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=t)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(t=!1){this.onSelectionChange.emit(new mf(this,t))}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Pe(FI,7),i&2){let o;S(o=k())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&ue("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Dt("id",r.id),ie("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),N("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",le]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:LI,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(re(PI),Z(0,VI,1,2,"mat-pseudo-checkbox",1),x(1),u(2,"span",2,0),x(4,1),f(),Z(5,BI,1,1,"mat-pseudo-checkbox",3),Z(6,jI,2,1,"span",4),W(7,"div",5)),i&2&&(Y(r.multiple?0:-1),z(5),Y(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),z(),Y(r.group&&r.group._inert?6:-1),z(),Fe("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[U_,Rv],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return e})();var nc=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J]})}return e})();var pf=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[On,nc,$_,J]})}return e})();var W_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[ff,pf,J,bi,Ci,pf]})}return e})();var hf=(()=>{class e{get vertical(){return this._vertical}set vertical(t){this._vertical=Te(t)}_vertical=!1;get inset(){return this._inset}set inset(t){this._inset=Te(t)}_inset=!1;static \u0275fac=function(i){return new(i||e)};static \u0275cmp=R({type:e,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(ie("aria-orientation",r.vertical?"vertical":"horizontal"),N("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2})}return e})(),ic=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J]})}return e})();var UI=["*"],$I=`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--mat-list-list-item-container-color, transparent);
  border-radius: var(--mat-list-list-item-container-shape, var(--mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--selected::before, .mdc-list-item.mdc-list-item--selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-leading-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-leading-icon-size, 24px);
  height: var(--mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--with-leading-icon:hover .mdc-list-item__start {
  color: var(--mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--mat-list-list-item-leading-avatar-size, 40px);
  height: var(--mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--mat-list-list-item-trailing-supporting-text-font, var(--mat-sys-label-small-font));
  line-height: var(--mat-list-list-item-trailing-supporting-text-line-height, var(--mat-sys-label-small-line-height));
  font-size: var(--mat-list-list-item-trailing-supporting-text-size, var(--mat-sys-label-small-size));
  font-weight: var(--mat-list-list-item-trailing-supporting-text-weight, var(--mat-sys-label-small-weight));
  letter-spacing: var(--mat-list-list-item-trailing-supporting-text-tracking, var(--mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-trailing-icon-size, 24px);
  height: var(--mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--with-trailing-icon:hover .mdc-list-item__end {
  color: var(--mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-supporting-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-selected-trailing-icon-color, var(--mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--mat-list-list-item-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-list-list-item-label-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-list-list-item-label-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-list-list-item-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-list-list-item-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-list-list-item-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-focus-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--mat-list-list-item-supporting-text-color, var(--mat-sys-on-surface-variant));
  font-family: var(--mat-list-list-item-supporting-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-list-list-item-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-list-list-item-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-list-list-item-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-list-list-item-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-disabled-leading-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-disabled-trailing-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-disabled-label-text-color, var(--mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--mat-list-list-item-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--disabled::before {
  background-color: var(--mat-list-list-item-disabled-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--mat-list-list-item-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--mat-list-list-item-leading-avatar-shape, var(--mat-sys-corner-full));
  background-color: var(--mat-list-list-item-leading-avatar-color, var(--mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
  --mat-focus-indicator-border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--mat-list-active-indicator-color, var(--mat-sys-secondary-container));
}
`,WI=["unscopedContent"],GI=["text"],qI=[[["","matListItemAvatar",""],["","matListItemIcon",""]],[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["","matListItemMeta",""]],[["mat-divider"]]],ZI=["[matListItemAvatar],[matListItemIcon]","[matListItemTitle]","[matListItemLine]","*","[matListItemMeta]","mat-divider"];var YI=new y("ListOption"),KI=(()=>{class e{_elementRef=m(B);static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return e})(),QI=(()=>{class e{_elementRef=m(B);static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return e})(),XI=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,selectors:[["","matListItemMeta",""]],hostAttrs:[1,"mat-mdc-list-item-meta","mdc-list-item__end"]})}return e})(),G_=(()=>{class e{_listOption=m(YI,{optional:!0});_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,hostVars:4,hostBindings:function(i,r){i&2&&N("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return e})(),JI=(()=>{class e extends G_{static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275dir=$({type:e,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[_e]})}return e})(),eM=(()=>{class e extends G_{static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275dir=$({type:e,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[_e]})}return e})(),tM=new y("MAT_LIST_CONFIG"),gf=(()=>{class e{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=Te(t)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(t){this._disabled.set(Te(t))}_disabled=Ee(!1);_defaultOptions=m(tM,{optional:!0});static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,hostVars:1,hostBindings:function(i,r){i&2&&ie("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return e})(),nM=(()=>{class e{_elementRef=m(B);_ngZone=m(C);_listBase=m(gf,{optional:!0});_platform=m(me);_hostElement;_isButtonElement;_noopAnimations=Ue();_avatars;_icons;set lines(t){this._explicitLines=an(t,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(t){this._disableRipple=Te(t)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(t){this._disabled.set(Te(t))}_disabled=Ee(!1);_subscriptions=new se;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){m(Le).load(cn);let t=m(xi,{optional:!0});this.rippleConfig=t||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new wi(this,this._ngZone,this._hostElement,this._platform,m(ee)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(Mt(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(t){if(!this._lines||!this._titles||!this._unscopedContent)return;t&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let t=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(t+=1),t}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(t=>t.nodeType!==t.COMMENT_NODE).some(t=>!!(t.textContent&&t.textContent.trim()))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=$({type:e,contentQueries:function(i,r,o){if(i&1&&He(o,JI,4)(o,eM,4),i&2){let s;S(s=k())&&(r._avatars=s),S(s=k())&&(r._icons=s)}},hostVars:4,hostBindings:function(i,r){i&2&&(ie("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),N("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return e})();var q_=(()=>{class e extends nM{_lines;_titles;_meta;_unscopedContent;_itemText;get activated(){return this._activated}set activated(t){this._activated=Te(t)}_activated=!1;_getAriaCurrent(){return this._hostElement.nodeName==="A"&&this._activated?"page":null}_hasBothLeadingAndTrailing(){return this._meta.length!==0&&(this._avatars.length!==0||this._icons.length!==0)}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-list-item"],["a","mat-list-item",""],["button","mat-list-item",""]],contentQueries:function(i,r,o){if(i&1&&He(o,QI,5)(o,KI,5)(o,XI,5),i&2){let s;S(s=k())&&(r._lines=s),S(s=k())&&(r._titles=s),S(s=k())&&(r._meta=s)}},viewQuery:function(i,r){if(i&1&&Pe(WI,5)(GI,5),i&2){let o;S(o=k())&&(r._unscopedContent=o.first),S(o=k())&&(r._itemText=o.first)}},hostAttrs:[1,"mat-mdc-list-item","mdc-list-item"],hostVars:13,hostBindings:function(i,r){i&2&&(ie("aria-current",r._getAriaCurrent()),N("mdc-list-item--activated",r.activated)("mdc-list-item--with-leading-avatar",r._avatars.length!==0)("mdc-list-item--with-leading-icon",r._icons.length!==0)("mdc-list-item--with-trailing-meta",r._meta.length!==0)("mat-mdc-list-item-both-leading-and-trailing",r._hasBothLeadingAndTrailing())("_mat-animation-noopable",r._noopAnimations))},inputs:{activated:"activated"},exportAs:["matListItem"],features:[_e],ngContentSelectors:ZI,decls:10,vars:0,consts:[["unscopedContent",""],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mat-focus-indicator"]],template:function(i,r){i&1&&(re(qI),x(0),u(1,"span",1),x(2,1),x(3,2),u(4,"span",2,0),ue("cdkObserveContent",function(){return r._updateItemLines(!0)}),x(6,3),f()(),x(7,4),x(8,5),W(9,"div",3))},dependencies:[bv],encapsulation:2})}return e})();var Z_=(()=>{class e extends gf{_isNonInteractive=!1;static \u0275fac=(()=>{let t;return function(r){return(t||(t=ke(e)))(r||e)}})();static \u0275cmp=R({type:e,selectors:[["mat-nav-list"]],hostAttrs:["role","navigation",1,"mat-mdc-nav-list","mat-mdc-list-base","mdc-list"],exportAs:["matNavList"],features:[Ce([{provide:gf,useExisting:e}]),_e],ngContentSelectors:UI,decls:1,vars:0,template:function(i,r){i&1&&(re(),x(0))},styles:[$I],encapsulation:2})}return e})();var Y_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[Ra,On,nc,J,ic]})}return e})();var K_=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=A({type:e});static \u0275inj=M({imports:[J]})}return e})();function rM(e,n){if(e&1){let t=Sn();u(0,"div",44)(1,"div",172)(2,"a",173)(3,"strong"),p(4,"Komunikat:"),f(),p(5," Bezp\u0142atna pomoc psychologiczna dla uchod\u017Ac\xF3w. "),f(),u(6,"button",174),ue("click",function(){Rt(t);let r=he();return Nt(r.announcementVisible=!1)}),u(7,"mat-icon"),p(8,"close"),f()()()()}}var rc=class e{announcementVisible=!0;scrollCarousel(n,t){let i=document.getElementById(n);i&&i.scrollBy({left:t==="next"?i.clientWidth*.8:-i.clientWidth*.8,behavior:"smooth"})}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=R({type:e,selectors:[["app-root"]],decls:655,vars:1,consts:[["sidenav",""],["href","#ptp-main-content",1,"ptp-skip-link"],[1,"ptp-app-container"],["mode","over","position","start",1,"ptp-sidenav"],[1,"ptp-sidenav-header"],[1,"ptp-sidenav-logo"],["mat-icon-button","","aria-label","Zamknij menu",3,"click"],["aria-label","Menu mobilne"],["mat-list-item","","href","#"],["role","banner",1,"ptp-header"],[1,"ptp-topbar"],[1,"ptp-topbar-inner","ptp-page-container"],[1,"ptp-topbar-left"],[1,"ptp-topbar-right"],[1,"ptp-lang-switcher"],["href","#","aria-current","true",1,"ptp-lang-btn","ptp-lang-btn--active"],[1,"ptp-lang-sep"],["href","#",1,"ptp-lang-btn"],["href","#",1,"ptp-topbar-contact"],["mat-icon-button","","aria-label","Moje konto",1,"ptp-topbar-icon"],[1,"ptp-header-inner","ptp-page-container"],["href","/","aria-label","Polskie Towarzystwo Psychoanalityczne \u2014 strona g\u0142\xF3wna",1,"ptp-logo-link"],["src","logo.svg","alt","PTPa","height","36",1,"ptp-logo-img"],["aria-label","Nawigacja g\u0142\xF3wna",1,"ptp-header-nav"],[1,"ptp-nav-item","ptp-nav-item--mega"],["href","#",1,"ptp-nav-link"],["role","menu",1,"ptp-mega-menu"],[1,"ptp-mega-menu__inner"],["href","#","role","menuitem",1,"ptp-mega-featured"],[1,"ptp-mega-featured__label"],[1,"ptp-mega-featured__title"],[1,"ptp-mega-featured__desc"],[1,"ptp-mega-featured__cta"],["aria-hidden","true",1,"ptp-mega-divider"],["role","group",1,"ptp-mega-links"],["href","#","role","menuitem",1,"ptp-mega-item"],[1,"ptp-mega-item__icon"],[1,"ptp-mega-item__text"],[1,"ptp-mega-item__title"],[1,"ptp-mega-item__desc"],["role","group",1,"ptp-mega-links","ptp-mega-links--3row"],[1,"ptp-mega-menu__inner","ptp-mega-menu__inner--no-featured"],["role","group",1,"ptp-mega-links","ptp-mega-links--1row"],["mat-icon-button","","aria-label","Otw\xF3rz menu","aria-haspopup","dialog",1,"ptp-mobile-menu-btn",3,"click"],["role","status","aria-live","polite",1,"ptp-announcement"],["id","ptp-main-content"],["aria-labelledby","hero-heading",1,"ptp-hero"],[1,"ptp-page-container"],[1,"ptp-hero-content"],["id","hero-heading",1,"ptp-hero-title"],[1,"ptp-hero-body"],[1,"ptp-hero-actions"],["mat-stroked-button","","href","#",1,"ptp-btn-hero-outline"],["mat-flat-button","","href","#",1,"ptp-btn-hero-filled"],["aria-hidden","true",1,"ptp-mosaic"],["viewBox","0 0 1200 240","xmlns","http://www.w3.org/2000/svg","preserveAspectRatio","xMidYMid slice"],["x","0","y","0","width","120","height","120","fill","#C89696"],["cx","180","cy","60","r","60","fill","#7D5252"],["points","240,0 360,0 360,120 240,120","fill","none","stroke","#EDD8D8","stroke-width","2"],["x","360","y","0","width","120","height","120","fill","#7D5252"],["cx","540","cy","60","r","60","fill","none","stroke","#C89696","stroke-width","2"],["x","600","y","0","width","60","height","60","fill","#EDD8D8"],["x","660","y","60","width","60","height","60","fill","#C89696"],["points","720,0 780,120 840,0","fill","#7D5252"],["x","840","y","0","width","120","height","120","fill","none","stroke","#7D5252","stroke-width","2"],["cx","1020","cy","60","r","60","fill","#C89696"],["x","1080","y","0","width","120","height","120","fill","#EDD8D8"],["x","0","y","120","width","120","height","120","fill","none","stroke","#C89696","stroke-width","2"],["x","120","y","120","width","60","height","60","fill","#7D5252"],["x","180","y","180","width","60","height","60","fill","#EDD8D8"],["cx","300","cy","180","r","60","fill","#C89696"],["points","360,120 480,120 420,240","fill","#7D5252"],["x","480","y","120","width","120","height","120","fill","#EDD8D8"],["cx","660","cy","180","r","60","fill","#7D5252"],["x","720","y","120","width","120","height","120","fill","none","stroke","#EDD8D8","stroke-width","2"],["points","840,240 960,120 1080,240","fill","#C89696"],["x","1080","y","120","width","120","height","120","fill","#7D5252"],["aria-labelledby","events-heading",1,"ptp-section","ptp-section--white"],[1,"ptp-section-header"],[1,"ptp-eyebrow"],["id","events-heading",1,"ptp-section-title"],["mat-stroked-button","","href","#",1,"ptp-see-all-btn"],["aria-label","Filtruj wydarzenia","multiple","",1,"ptp-filter-bar"],["selected","",1,"ptp-filter-chip"],[1,"ptp-filter-chip"],[1,"ptp-carousel-outer"],["id","eventCarousel","role","list",1,"ptp-event-carousel"],["role","listitem",1,"ptp-event-card"],[1,"ptp-event-card__image","ptp-event-card__image--1"],[1,"ptp-label","ptp-label--rose"],[1,"ptp-event-card__body"],[1,"ptp-event-card__title"],[1,"ptp-event-card__desc"],[1,"ptp-event-card__footer"],[1,"ptp-meta-item"],["aria-hidden","true",1,"ptp-meta-icon"],[1,"ptp-event-card__image","ptp-event-card__image--2"],[1,"ptp-label","ptp-label--sage"],[1,"ptp-event-card__image","ptp-event-card__image--3"],[1,"ptp-event-card__image","ptp-event-card__image--4"],[1,"ptp-carousel-nav"],["mat-icon-button","","aria-label","Poprzednie",3,"click"],["mat-icon-button","","aria-label","Nast\u0119pne",3,"click"],["aria-labelledby","komunikaty-heading",1,"ptp-section","ptp-section--cream"],["id","komunikaty-heading",1,"ptp-section-title"],["id","komunikatyCarousel","role","list",1,"ptp-komunikaty-carousel"],["role","listitem",1,"ptp-komunikat-card"],[1,"ptp-komunikat-card__title"],[1,"ptp-komunikat-card__desc"],[1,"ptp-komunikat-card__footer"],["mat-stroked-button","","href","#",1,"ptp-komunikat-card__btn"],["aria-labelledby","training-heading",1,"ptp-section","ptp-section--white"],["id","training-heading",1,"ptp-section-title"],["role","list",1,"ptp-training-grid"],["role","listitem",1,"ptp-training-card","ptp-training-card--1"],[1,"ptp-training-card__inner"],[1,"ptp-training-card__title"],["role","listitem",1,"ptp-training-card","ptp-training-card--2"],["role","listitem",1,"ptp-training-card","ptp-training-card--3"],[1,"ptp-quote"],["aria-hidden","true",1,"ptp-quote__mark"],[1,"ptp-quote__text"],[1,"ptp-quote__attribution"],["aria-hidden","true",1,"ptp-quote__line"],[1,"ptp-quote__cite"],["aria-labelledby","newsletter-heading",1,"ptp-section","ptp-newsletter"],[1,"ptp-newsletter-inner"],[1,"ptp-newsletter-left"],[1,"ptp-newsletter__eyebrow"],["id","newsletter-heading",1,"ptp-newsletter__title"],[1,"ptp-newsletter__desc"],[1,"ptp-newsletter-right"],["aria-label","Formularz zapisu na newsletter",1,"ptp-newsletter__form",3,"submit"],["type","email","placeholder","Tw\xF3j adres email","aria-label","Adres email","autocomplete","email",1,"ptp-newsletter__input"],["mat-flat-button","","type","submit",1,"ptp-newsletter__btn"],[1,"ptp-newsletter__note"],["aria-labelledby","cta-heading",1,"ptp-cta-section"],[1,"ptp-cta-inner"],[1,"ptp-cta-left"],[1,"ptp-cta-eyebrow"],["id","cta-heading",1,"ptp-cta-title"],[1,"ptp-cta-right"],[1,"ptp-cta-desc"],[1,"ptp-cta-actions"],["mat-flat-button","","href","#",1,"ptp-cta-btn--filled"],["mat-stroked-button","","href","#",1,"ptp-cta-btn--outline"],[1,"ptp-footer"],[1,"ptp-footer-grid"],[1,"ptp-footer-col","ptp-footer-col--brand"],["src","logo-white.svg","alt","PTPa","height","48",1,"ptp-logo-img","ptp-logo-img--footer"],[1,"ptp-footer-brand-desc"],[1,"ptp-footer-social"],["href","https://facebook.com","aria-label","Facebook","target","_blank","rel","noopener",1,"ptp-footer-social__link"],["viewBox","0 0 24 24","xmlns","http://www.w3.org/2000/svg","aria-hidden","true"],["d","M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"],["href","https://linkedin.com","aria-label","LinkedIn","target","_blank","rel","noopener",1,"ptp-footer-social__link"],["d","M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"],[1,"ptp-footer-col"],[1,"ptp-footer-col__title"],[1,"ptp-footer-col__list"],["href","#"],[1,"ptp-footer-col__title","ptp-footer-col__title--spaced"],[1,"ptp-footer-address"],[1,"ptp-footer-address__row"],["href","mailto:biuro@ptpa.org.pl"],[1,"ptp-footer-ipa"],[1,"ptp-footer-ipa__label"],["src","ipa-logo.png","alt","International Psychoanalytical Association",1,"ptp-footer-ipa__logo"],[1,"ptp-footer-divider"],[1,"ptp-footer-bottom"],[1,"ptp-footer-copy"],["aria-label","Informacje prawne",1,"ptp-footer-legal"],[1,"ptp-page-container","ptp-announcement-inner"],["href","#",1,"ptp-announcement__text"],["mat-icon-button","","aria-label","Zamknij komunikat",1,"ptp-announcement__close",3,"click"]],template:function(t,i){if(t&1){let r=Sn();u(0,"a",1),p(1,"Przejd\u017A do tre\u015Bci g\u0142\xF3wnej"),f(),u(2,"mat-sidenav-container",2)(3,"mat-sidenav",3,0)(5,"div",4)(6,"span",5),p(7,"PTPa"),f(),u(8,"button",6),ue("click",function(){Rt(r);let s=Bt(4);return Nt(s.close())}),u(9,"mat-icon"),p(10,"close"),f()()(),u(11,"nav",7)(12,"mat-nav-list")(13,"a",8),p(14,"Towarzystwo"),f(),u(15,"a",8),p(16,"Aktualno\u015Bci"),f(),u(17,"a",8),p(18,"Psychoanaliza"),f(),u(19,"a",8),p(20,"Biblioteka i Media"),f(),u(21,"a",8),p(22,"Szkolenie"),f(),W(23,"mat-divider"),u(24,"a",8),p(25,"Kontakt"),f(),u(26,"a",8),p(27,"PL | EN"),f()()()(),u(28,"mat-sidenav-content")(29,"header",9)(30,"div",10)(31,"div",11),W(32,"div",12),u(33,"div",13)(34,"div",14)(35,"a",15),p(36,"PL"),f(),u(37,"span",16),p(38,"|"),f(),u(39,"a",17),p(40,"EN"),f()(),u(41,"a",18),p(42,"Kontakt"),f(),u(43,"button",19)(44,"mat-icon"),p(45,"person_outline"),f()()()()(),u(46,"div",20)(47,"a",21),W(48,"img",22),f(),u(49,"nav",23)(50,"div",24)(51,"a",25),p(52,"Towarzystwo"),f(),u(53,"div",26)(54,"div",27)(55,"a",28)(56,"span",29),p(57,"O nas"),f(),u(58,"strong",30),p(59,"Polskie Towarzystwo Psychoanalityczne"),f(),u(60,"p",31),p(61,"Zrzeszamy psychoanalityk\xF3w i psychoterapeut\xF3w w Polsce od 1989 roku."),f(),u(62,"span",32),p(63,"Dowiedz si\u0119 wi\u0119cej "),u(64,"mat-icon"),p(65,"arrow_forward"),f()()(),W(66,"div",33),u(67,"div",34)(68,"a",35)(69,"div",36)(70,"mat-icon"),p(71,"gavel"),f()(),u(72,"div",37)(73,"span",38),p(74,"Kodeks Etyczny"),f(),u(75,"span",39),p(76,"Standardy merytoryczne i etyczne"),f()()(),u(77,"a",35)(78,"div",36)(79,"mat-icon"),p(80,"account_balance"),f()(),u(81,"div",37)(82,"span",38),p(83,"W\u0142adze"),f(),u(84,"span",39),p(85,"Zarz\u0105d, Komisje oraz Podkomisje"),f()()(),u(86,"a",35)(87,"div",36)(88,"mat-icon"),p(89,"verified_user"),f()(),u(90,"div",37)(91,"span",38),p(92,"Cz\u0142onkowie"),f(),u(93,"span",39),p(94,"Licencjonowani psychoanalitycy"),f()()(),u(95,"a",35)(96,"div",36)(97,"mat-icon"),p(98,"school"),f()(),u(99,"div",37)(100,"span",38),p(101,"Kandydaci"),f(),u(102,"span",39),p(103,"W trakcie szkolenia psychoanalitycznego"),f()()(),u(104,"a",35)(105,"div",36)(106,"mat-icon"),p(107,"military_tech"),f()(),u(108,"div",37)(109,"span",38),p(110,"Cz\u0142onkowie honorowi"),f(),u(111,"span",39),p(112,"Osoby zas\u0142u\u017Cone dla Towarzystwa"),f()()(),u(113,"a",35)(114,"div",36)(115,"mat-icon"),p(116,"favorite_border"),f()(),u(117,"div",37)(118,"span",38),p(119,"Po\u017Cegnania"),f(),u(120,"span",39),p(121,"Wspomnienia o zmar\u0142ych cz\u0142onkach PTPa"),f()()()()()()(),u(122,"div",24)(123,"a",25),p(124,"Aktualno\u015Bci"),f(),u(125,"div",26)(126,"div",27)(127,"a",28)(128,"span",29),p(129,"Aktualno\u015Bci"),f(),u(130,"strong",30),p(131,"Wydarzenia"),W(132,"br"),p(133,"i komunikaty PTPa"),f(),u(134,"p",31),p(135,"Wyk\u0142ady, konferencje, kursy i wszystkie aktualno\u015Bci Towarzystwa."),f(),u(136,"span",32),p(137,"Wszystkie aktualno\u015Bci "),u(138,"mat-icon"),p(139,"arrow_forward"),f()()(),W(140,"div",33),u(141,"div",40)(142,"a",35)(143,"div",36)(144,"mat-icon"),p(145,"grid_view"),f()(),u(146,"div",37)(147,"span",38),p(148,"Wszystkie wydarzenia"),f(),u(149,"span",39),p(150,"Organizowane przez Towarzystwo"),f()()(),u(151,"a",35)(152,"div",36)(153,"mat-icon"),p(154,"mic"),f()(),u(155,"div",37)(156,"span",38),p(157,"Wyk\u0142ady"),f(),u(158,"span",39),p(159,"Wyk\u0142ady otwarte PTPa"),f()()(),u(160,"a",35)(161,"div",36)(162,"mat-icon"),p(163,"event"),f()(),u(164,"div",37)(165,"span",38),p(166,"Seminaria"),f(),u(167,"span",39),p(168,"Otwarte seminaria PTPa"),f()()(),u(169,"a",35)(170,"div",36)(171,"mat-icon"),p(172,"school"),f()(),u(173,"div",37)(174,"span",38),p(175,"Kursy"),f(),u(176,"span",39),p(177,"Kursy psychoanalityczne"),f()()(),u(178,"a",35)(179,"div",36)(180,"mat-icon"),p(181,"groups_2"),f()(),u(182,"div",37)(183,"span",38),p(184,"Konferencje"),f(),u(185,"span",39),p(186,"Konferencje PTPa"),f()()(),u(187,"a",35)(188,"div",36)(189,"mat-icon"),p(190,"forum"),f()(),u(191,"div",37)(192,"span",38),p(193,"Sympozja"),f(),u(194,"span",39),p(195,"Sympozja PTPa"),f()()(),u(196,"a",35)(197,"div",36)(198,"mat-icon"),p(199,"wb_sunny"),f()(),u(200,"div",37)(201,"span",38),p(202,"Letnia szko\u0142a psychoanalizy"),f(),u(203,"span",39),p(204,"Letnie wydarzenie promuj\u0105ce psychoanaliz\u0119"),f()()(),u(205,"a",35)(206,"div",36)(207,"mat-icon"),p(208,"info_outline"),f()(),u(209,"div",37)(210,"span",38),p(211,"Komunikaty"),f(),u(212,"span",39),p(213,"Aktualno\u015Bci w Towarzystwie"),f()()(),u(214,"a",35)(215,"div",36)(216,"mat-icon"),p(217,"menu_book"),f()(),u(218,"div",37)(219,"span",38),p(220,"Publikacje"),f(),u(221,"span",39),p(222,"Publikacje Towarzystwa"),f()()()()()()(),u(223,"div",24)(224,"a",25),p(225,"Psychoanaliza"),f(),u(226,"div",26)(227,"div",41)(228,"div",42)(229,"a",35)(230,"div",36)(231,"mat-icon"),p(232,"psychology"),f()(),u(233,"div",37)(234,"span",38),p(235,"O psychoanalizie"),f(),u(236,"span",39),p(237,"Teoria funkcjonowania ludzkiego umys\u0142u"),f()()(),u(238,"a",35)(239,"div",36)(240,"mat-icon"),p(241,"person_search"),f()(),u(242,"div",37)(243,"span",38),p(244,"Znajd\u017A psychoanalityka"),f(),u(245,"span",39),p(246,"Znajd\u017A do\u015Bwiadczonych psychoanalityk\xF3w"),f()()()()()()(),u(247,"a",25),p(248,"Biblioteka i Media"),f(),u(249,"a",25),p(250,"Szkolenie"),f()(),u(251,"button",43),ue("click",function(){Rt(r);let s=Bt(4);return Nt(s.toggle())}),u(252,"mat-icon"),p(253,"menu"),f()()()(),Z(254,rM,9,0,"div",44),u(255,"main",45)(256,"section",46)(257,"div",47)(258,"div",48)(259,"h1",49),p(260,"Polskie Towarzystwo Psychoanalityczne"),f(),u(261,"p",50),p(262," Zrzeszamy psychoanalityk\xF3w i psychoterapeut\xF3w psychoanalitycznych w Polsce. Wspieramy rozw\xF3j psychoanalizy, prowadz\u0105c szkolenia, dzia\u0142alno\u015B\u0107 naukow\u0105 i kulturaln\u0105. "),f(),u(263,"div",51)(264,"a",52),p(265,"Szkolenie w psychoanalizie"),f(),u(266,"a",53),p(267,"Znajd\u017A psychoanalityka"),f()()()()(),u(268,"div",54),vt(),u(269,"svg",55),W(270,"rect",56)(271,"circle",57)(272,"polygon",58)(273,"rect",59)(274,"circle",60)(275,"rect",61)(276,"rect",62)(277,"polygon",63)(278,"rect",64)(279,"circle",65)(280,"rect",66)(281,"rect",67)(282,"rect",68)(283,"rect",69)(284,"circle",70)(285,"polygon",71)(286,"rect",72)(287,"circle",73)(288,"rect",74)(289,"polygon",75)(290,"rect",76),f()(),Dn(),u(291,"section",77)(292,"div",47)(293,"div",78)(294,"div")(295,"p",79),p(296,"Aktualno\u015Bci"),f(),u(297,"h2",80),p(298,"Wydarzenia"),f()(),u(299,"a",81),p(300,"Zobacz wszystkie wydarzenia"),f()(),u(301,"mat-chip-listbox",82)(302,"mat-chip-option",83),p(303,"Wszystkie"),f(),u(304,"mat-chip-option",84),p(305,"Letnia Szko\u0142a"),f(),u(306,"mat-chip-option",84),p(307,"Kursy"),f(),u(308,"mat-chip-option",84),p(309,"Konferencje"),f(),u(310,"mat-chip-option",84),p(311,"Warsztaty"),f()(),u(312,"div",85)(313,"div",86)(314,"article",87)(315,"div",88)(316,"span",89),p(317,"Letnia Szko\u0142a Psychoanalizy"),f()(),u(318,"div",90)(319,"h3",91),p(320,"Cia\u0142o, Seksualno\u015B\u0107, Sztuczna Doskona\u0142o\u015B\u0107"),f(),u(321,"p",92),p(322," Polskie Towarzystwo Psychoanalityczne zaprasza na III edycj\u0119 Letniej Szko\u0142y Psychoanalizy, kt\xF3ra odb\u0119dzie si\u0119 w przestrzeni Starego BUW-u w Warszawie. "),f(),u(323,"div",93)(324,"span",94)(325,"mat-icon",95),p(326,"calendar_today"),f(),p(327,"18\u201320 wrze\u015Bnia 2026"),f(),u(328,"span",94)(329,"mat-icon",95),p(330,"location_on"),f(),p(331,"Na miejscu"),f()()()(),u(332,"article",87)(333,"div",96)(334,"span",97),p(335,"Kurs"),f()(),u(336,"div",90)(337,"h3",91),p(338,"Jak powstaje \u017Cycie psychiczne. Podstawowe idee Melanie Klein."),f(),u(339,"p",92),p(340," Chcemy zaprosi\u0107 Pa\u0144stwa do wsp\xF3lnej podr\xF3\u017Cy, podczas kt\xF3rej poznawa\u0107 b\u0119dziemy teori\u0119 wybitnej psychoanalityczki zwi\u0105zanej z Brytyjskim Towarzystwem Psychoanalitycznym. "),f(),u(341,"div",93)(342,"span",94)(343,"mat-icon",95),p(344,"calendar_today"),f(),p(345,"03 pa\u017Adziernika 2026"),f(),u(346,"span",94)(347,"mat-icon",95),p(348,"location_on"),f(),p(349,"Na miejscu"),f()()()(),u(350,"article",87)(351,"div",98)(352,"span",89),p(353,"Wyk\u0142ad otwarty"),f()(),u(354,"div",90)(355,"h3",91),p(356,"Psychoanaliza wobec kryzysu klimatycznego"),f(),u(357,"p",92),p(358," Zapraszamy na wyk\u0142ad otwarty po\u015Bwi\u0119cony psychoanalitycznym perspektywom rozumienia l\u0119ku ekologicznego, \u017Ca\u0142oby klimatycznej i zbiorowej odpowiedzi na kryzys. "),f(),u(359,"div",93)(360,"span",94)(361,"mat-icon",95),p(362,"calendar_today"),f(),p(363,"15 listopada 2026"),f(),u(364,"span",94)(365,"mat-icon",95),p(366,"location_on"),f(),p(367,"Online"),f()()()(),u(368,"article",87)(369,"div",99)(370,"span",97),p(371,"Konferencja"),f()(),u(372,"div",90)(373,"h3",91),p(374,"Konferencja Europejskiego Towarzystwa Psychoanalizy 2027"),f(),u(375,"p",92),p(376,' Coroczna konferencja EPF po\u015Bwi\u0119cona wsp\xF3\u0142czesnym wyzwaniom w praktyce i teorii psychoanalitycznej. Tegoroczne has\u0142o: \u201ETo\u017Csamo\u015B\u0107 i transformacja". '),f(),u(377,"div",93)(378,"span",94)(379,"mat-icon",95),p(380,"calendar_today"),f(),p(381,"14\u201316 marca 2027"),f(),u(382,"span",94)(383,"mat-icon",95),p(384,"location_on"),f(),p(385,"Warszawa"),f()()()()()(),u(386,"div",100)(387,"button",101),ue("click",function(){return i.scrollCarousel("eventCarousel","prev")}),u(388,"mat-icon"),p(389,"chevron_left"),f()(),u(390,"button",102),ue("click",function(){return i.scrollCarousel("eventCarousel","next")}),u(391,"mat-icon"),p(392,"chevron_right"),f()()()()(),u(393,"section",103)(394,"div",47)(395,"div",78)(396,"div")(397,"p",79),p(398,"Aktualno\u015Bci"),f(),u(399,"h2",104),p(400,"Komunikaty"),f()(),u(401,"a",81),p(402,"Zobacz wszystkie komunikaty"),f()(),u(403,"div",85)(404,"div",105)(405,"article",106)(406,"h3",107),p(407,"Pan Profesor Pawe\u0142 Dybel nowym Cz\u0142onkiem Honorowym w PTPa"),f(),u(408,"p",108),p(409,"W dniu 03.04.2024 po prowadzonym przez siebie wyk\u0142adzie otwartym Pan Profesor Piotr Dybel zosta\u0142 przyj\u0119ty w poczet Cz\u0142onk\xF3w Honorowych w Polskim Towarzystwie Psychoanalitycznym."),f(),u(410,"div",109)(411,"span",94)(412,"mat-icon",95),p(413,"calendar_today"),f(),p(414,"\u015Broda, 03 kwietnia 2024"),f(),u(415,"a",110),p(416,"Zobacz wi\u0119cej"),f()()(),u(417,"article",106)(418,"h3",107),p(419,"Polskie Towarzystwo przy\u0142\u0105cza si\u0119 do stanowiska Mi\u0119dzynarodowej grupy studi\xF3w nad antysemityzmem."),f(),u(420,"p",108),p(421,"Polskie Towarzystwo przy\u0142\u0105cza si\u0119 do stanowiska Mi\u0119dzynarodowej grupy studi\xF3w nad antysemityzmem i wyra\u017Ca sprzeciw wobec wszelkich form dyskryminacji."),f(),u(422,"div",109)(423,"span",94)(424,"mat-icon",95),p(425,"calendar_today"),f(),p(426,"\u015Broda, 01 listopada 2023"),f(),u(427,"a",110),p(428,"Zobacz wi\u0119cej"),f()()(),u(429,"article",106)(430,"h3",107),p(431,"Stanowisko stowarzysze\u0144 zrzeszaj\u0105cych psychoterapeut\xF3w w sprawie specjalizacji w psychoterapii"),f(),u(432,"p",108),p(433,"Chcemy zwr\xF3ci\u0107 uwag\u0119 na ryzyka i problemy z jakimi, w zwi\u0105zku z nowym rozporz\u0105dzeniem, b\u0119d\u0105 si\u0119 spotyka\u0107 osoby korzystaj\u0105ce z psychoterapii."),f(),u(434,"div",109)(435,"span",94)(436,"mat-icon",95),p(437,"calendar_today"),f(),p(438,"pi\u0105tek, 30 czerwca 2023"),f(),u(439,"a",110),p(440,"Zobacz wi\u0119cej"),f()()(),u(441,"article",106)(442,"h3",107),p(443,"dr Zbigniew Sokolik"),f(),u(444,"p",108),p(445,"Ze smutkiem i \u017Calem informujemy, \u017Ce umar\u0142 dr Zbigniew Sokolik \u2013 nestor i jeden z pionier\xF3w psychoanalizy w powojennej Polsce, cz\u0142onek honorowy PTPa."),f(),u(446,"div",109)(447,"span",94)(448,"mat-icon",95),p(449,"calendar_today"),f(),p(450,"niedziela, 11 wrze\u015Bnia 2022"),f(),u(451,"a",110),p(452,"Zobacz wi\u0119cej"),f()()(),u(453,"article",106)(454,"h3",107),p(455,"Zmiany w zarz\u0105dzie Polskiego Towarzystwa Psychoanalitycznego"),f(),u(456,"p",108),p(457,"Informujemy, \u017Ce na ostatnim Walnym Zebraniu Cz\u0142onk\xF3w PTPa dokonano wyboru nowych w\u0142adz Towarzystwa na kadencj\u0119 2024\u20132027."),f(),u(458,"div",109)(459,"span",94)(460,"mat-icon",95),p(461,"calendar_today"),f(),p(462,"poniedzia\u0142ek, 12 lutego 2024"),f(),u(463,"a",110),p(464,"Zobacz wi\u0119cej"),f()()(),u(465,"article",106)(466,"h3",107),p(467,"Wyniki wybor\xF3w do Komitetu Naukowego PTPa"),f(),u(468,"p",108),p(469,"Podajemy do wiadomo\u015Bci wyniki wybor\xF3w uzupe\u0142niaj\u0105cych do Komitetu Naukowego Polskiego Towarzystwa Psychoanalitycznego przeprowadzonych w styczniu 2024."),f(),u(470,"div",109)(471,"span",94)(472,"mat-icon",95),p(473,"calendar_today"),f(),p(474,"czwartek, 25 stycznia 2024"),f(),u(475,"a",110),p(476,"Zobacz wi\u0119cej"),f()()()()(),u(477,"div",100)(478,"button",101),ue("click",function(){return i.scrollCarousel("komunikatyCarousel","prev")}),u(479,"mat-icon"),p(480,"chevron_left"),f()(),u(481,"button",102),ue("click",function(){return i.scrollCarousel("komunikatyCarousel","next")}),u(482,"mat-icon"),p(483,"chevron_right"),f()()()()(),u(484,"section",111)(485,"div",47)(486,"div",78)(487,"div")(488,"p",79),p(489,"Szkolenie psychoanalityczne"),f(),u(490,"h2",112),p(491,"Jak zosta\u0107 Psychoanalitykiem"),f()(),u(492,"a",81),p(493,"Zobacz wi\u0119cej"),f()(),u(494,"div",113)(495,"article",114)(496,"div",115)(497,"h3",116),p(498,"W\u0142asna analiza"),f()()(),u(499,"article",117)(500,"div",115)(501,"h3",116),p(502,"Superwizja kliniczna"),f()()(),u(503,"article",118)(504,"div",115)(505,"h3",116),p(506,"Seminaria teoretyczne"),f()()()(),u(507,"figure",119)(508,"span",120),p(509,"\u201C"),f(),u(510,"blockquote",121),p(511," Celem psychoanalizy jest zast\u0105pienie nieuchronnego nieszcz\u0119\u015Bcia zwyk\u0142\u0105, ludzk\u0105 niedol\u0105. "),f(),u(512,"figcaption",122),W(513,"div",123),u(514,"cite",124),p(515,"Sigmund Freud"),f()()()()(),u(516,"section",125)(517,"div",47)(518,"div",126)(519,"div",127)(520,"p",128),p(521,"Newsletter"),f(),u(522,"h2",129),p(523,"B\u0105d\u017A na bie\u017C\u0105co z psychoanaliz\u0105"),f(),u(524,"p",130),p(525,"Otrzymuj informacje o wydarzeniach, publikacjach i dzia\u0142alno\u015Bci Polskiego Towarzystwa Psychoanalitycznego."),f()(),u(526,"div",131)(527,"form",132),ue("submit",function(s){return s.preventDefault()}),W(528,"input",133),u(529,"button",134),p(530,"Zapisz si\u0119"),f()(),u(531,"p",135),p(532,"Dbamy o Twoje dane zgodnie z polityk\u0105 prywatno\u015Bci. Mo\u017Cesz zrezygnowa\u0107 w ka\u017Cdej chwili."),f()()()()(),u(533,"section",136)(534,"div",47)(535,"div",137)(536,"div",138)(537,"p",139),p(538,"Kontakt"),f(),u(539,"h2",140),p(540,"Zacznij swoj\u0105"),W(541,"br"),p(542,"podr\xF3\u017C w g\u0142\u0105b siebie"),f()(),u(543,"div",141)(544,"p",142),p(545," Skontaktuj si\u0119 z nami i dowiedz si\u0119, jak psychoanaliza mo\u017Ce Ci pom\xF3c. Nasze Towarzystwo skupia licencjonowanych psychoanalityk\xF3w w ca\u0142ej Polsce. "),f(),u(546,"div",143)(547,"a",144),p(548,"Przejd\u017A do kontaktu"),f(),u(549,"a",145),p(550,"Znajd\u017A psychoanalityka"),f()()()()()()(),u(551,"footer",146)(552,"div",47)(553,"div",147)(554,"div",148),W(555,"img",149),u(556,"p",150),p(557,"Zrzeszamy psychoanalityk\xF3w i psychoterapeut\xF3w psychoanalitycznych w Polsce od 1989 roku. Jeste\u015Bmy cz\u0142onkiem Mi\u0119dzynarodowego Stowarzyszenia Psychoanalitycznego (IPA)."),f(),u(558,"div",151)(559,"a",152),vt(),u(560,"svg",153),W(561,"path",154),f()(),Dn(),u(562,"a",155),vt(),u(563,"svg",153),W(564,"path",156),f()()()(),Dn(),u(565,"div",157)(566,"h3",158),p(567,"Towarzystwo"),f(),u(568,"ul",159)(569,"li")(570,"a",160),p(571,"O nas"),f()(),u(572,"li")(573,"a",160),p(574,"Kodeks Etyczny"),f()(),u(575,"li")(576,"a",160),p(577,"W\u0142adze"),f()(),u(578,"li")(579,"a",160),p(580,"Cz\u0142onkowie"),f()(),u(581,"li")(582,"a",160),p(583,"Kandydaci"),f()(),u(584,"li")(585,"a",160),p(586,"Po\u017Cegnania"),f()()()(),u(587,"div",157)(588,"h3",158),p(589,"Aktualno\u015Bci"),f(),u(590,"ul",159)(591,"li")(592,"a",160),p(593,"Wszystkie wydarzenia"),f()(),u(594,"li")(595,"a",160),p(596,"Wyk\u0142ady"),f()(),u(597,"li")(598,"a",160),p(599,"Seminaria"),f()(),u(600,"li")(601,"a",160),p(602,"Konferencje"),f()(),u(603,"li")(604,"a",160),p(605,"Komunikaty"),f()(),u(606,"li")(607,"a",160),p(608,"Publikacje"),f()()(),u(609,"h3",161),p(610,"Psychoanaliza"),f(),u(611,"ul",159)(612,"li")(613,"a",160),p(614,"O psychoanalizie"),f()(),u(615,"li")(616,"a",160),p(617,"Znajd\u017A psychoanalityka"),f()(),u(618,"li")(619,"a",160),p(620,"Szkolenie"),f()(),u(621,"li")(622,"a",160),p(623,"Biblioteka i Media"),f()()()(),u(624,"div",157)(625,"h3",158),p(626,"Kontakt"),f(),u(627,"address",162)(628,"div",163)(629,"mat-icon"),p(630,"location_on"),f(),u(631,"span"),p(632,"ul. Ho\u017Cej 26 m. 6"),W(633,"br"),p(634,"00-528 Warszawa"),f()(),u(635,"div",163)(636,"mat-icon"),p(637,"mail_outline"),f(),u(638,"a",164),p(639,"biuro@ptpa.org.pl"),f()()(),u(640,"div",165)(641,"p",166),p(642,"Cz\u0142onek"),f(),W(643,"img",167),f()()(),W(644,"mat-divider",168),u(645,"div",169)(646,"p",170),p(647,"\xA9 2026 Polskie Towarzystwo Psychoanalityczne"),f(),u(648,"nav",171)(649,"a",160),p(650,"Polityka prywatno\u015Bci"),f(),u(651,"a",160),p(652,"RODO"),f(),u(653,"a",160),p(654,"Dost\u0119pno\u015B\u0107"),f()()()()()()()}t&2&&(z(254),Y(i.announcementVisible?254:-1))},dependencies:[uv,Sv,zu,Mv,Ba,Vv,Lv,qu,Gv,Wv,qv,y_,nf,tf,Ci,w_,W_,Y_,Z_,q_,hf,ic,K_],styles:["[_nghost-%COMP%]{display:block;height:100%}.ptp-app-container[_ngcontent-%COMP%]{height:100%;min-height:100vh}.ptp-page-container[_ngcontent-%COMP%]{max-width:1200px;margin:0 auto;padding:0 32px}@media(max-width:599px){.ptp-page-container[_ngcontent-%COMP%]{padding:0 16px}}.ptp-topbar[_ngcontent-%COMP%]{background:var(--md-sys-color-on-surface);color:var(--md-sys-color-inverse-on-surface)}.ptp-topbar-inner[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;height:36px}.ptp-topbar-right[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px}.ptp-lang-switcher[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px;font-size:12px;font-weight:500;letter-spacing:.05em}.ptp-lang-btn[_ngcontent-%COMP%]{color:var(--md-sys-color-inverse-on-surface);text-decoration:none;opacity:.6}.ptp-lang-btn--active[_ngcontent-%COMP%]{opacity:1;font-weight:700}.ptp-lang-sep[_ngcontent-%COMP%]{opacity:.3;font-size:11px}.ptp-topbar-contact[_ngcontent-%COMP%]{font-size:12px;font-weight:500;color:var(--md-sys-color-inverse-on-surface);text-decoration:none;opacity:.7;letter-spacing:.03em}.ptp-topbar-contact[_ngcontent-%COMP%]:hover{opacity:1}.ptp-topbar-icon[_ngcontent-%COMP%]{color:var(--md-sys-color-inverse-on-surface)!important;--mdc-icon-button-state-layer-size: 32px !important;--mat-icon-button-state-layer-size: 32px !important;width:32px!important;height:32px!important;padding:0!important;display:inline-flex!important;align-items:center!important;justify-content:center!important}.ptp-topbar-icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:20px!important;width:20px!important;height:20px!important;line-height:20px!important}.ptp-header[_ngcontent-%COMP%]{position:sticky;top:0;z-index:200;background:var(--md-sys-color-surface-container-lowest);border-bottom:1px solid var(--md-sys-color-outline-variant)}.ptp-announcement[_ngcontent-%COMP%]{background:var(--md-sys-color-primary);color:var(--md-sys-color-on-primary)}.ptp-announcement-inner[_ngcontent-%COMP%]{display:flex;align-items:center;min-height:44px;gap:8px}.ptp-announcement__text[_ngcontent-%COMP%]{flex:1;font-size:13px;text-align:center;color:var(--md-sys-color-on-primary);text-decoration:none}.ptp-announcement__text[_ngcontent-%COMP%]:hover{text-decoration:underline}.ptp-announcement__text[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{font-weight:600}.ptp-announcement__close[_ngcontent-%COMP%]{flex:0 0 auto;color:var(--md-sys-color-on-primary)!important;--mdc-icon-button-state-layer-size: 32px !important;--mat-icon-button-state-layer-size: 32px !important;width:32px!important;height:32px!important;padding:0!important;display:inline-flex!important;align-items:center!important;justify-content:center!important}.ptp-announcement__close[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:20px!important;width:20px!important;height:20px!important;line-height:20px!important}.ptp-header-inner[_ngcontent-%COMP%]{display:flex;align-items:center;height:64px;gap:16px;position:relative}.ptp-logo-link[_ngcontent-%COMP%]{text-decoration:none;display:flex;align-items:center;margin-right:auto}.ptp-logo-img[_ngcontent-%COMP%]{display:block;height:36px;width:auto}.ptp-logo-img--footer[_ngcontent-%COMP%]{height:48px;opacity:.95}.ptp-header-nav[_ngcontent-%COMP%]{display:flex;gap:0;align-items:center}@media(max-width:904px){.ptp-header-nav[_ngcontent-%COMP%]{display:none}}.ptp-mobile-menu-btn[_ngcontent-%COMP%]{display:none}@media(max-width:904px){.ptp-mobile-menu-btn[_ngcontent-%COMP%]{display:inline-flex}}.ptp-nav-link[_ngcontent-%COMP%]{display:inline-flex;align-items:center;height:64px;padding:0 16px;font-family:Inter,sans-serif;font-size:14px;font-weight:500;color:var(--md-sys-color-on-surface);letter-spacing:.01em;text-decoration:none;border-bottom:2px solid transparent;transition:color .15s ease,border-color .15s ease}.ptp-nav-link[_ngcontent-%COMP%]:hover{color:var(--md-sys-color-primary);border-bottom-color:var(--md-sys-color-primary)}.ptp-mega-menu[_ngcontent-%COMP%]{position:absolute;top:100%;left:calc(-1 * var(--ptp-layout-page-padding));right:calc(-1 * var(--ptp-layout-page-padding));background:var(--md-sys-color-surface-container-lowest);border:1px solid rgba(0,0,0,.08);border-radius:20px;box-shadow:0 16px 48px #0000001a,0 4px 12px #0000000f;overflow:hidden;opacity:0;pointer-events:none;transform:translateY(-6px);transition:opacity .2s cubic-bezier(.2,0,0,1),transform .2s cubic-bezier(.2,0,0,1);z-index:300}.ptp-nav-item--mega[_ngcontent-%COMP%]:hover   .ptp-mega-menu[_ngcontent-%COMP%]{opacity:1;pointer-events:all;transform:translateY(0)}.ptp-mega-menu__inner[_ngcontent-%COMP%]{display:flex;align-items:stretch}.ptp-mega-menu__inner--no-featured[_ngcontent-%COMP%]   .ptp-mega-links[_ngcontent-%COMP%]{padding:16px var(--ptp-layout-page-padding)}.ptp-mega-featured[_ngcontent-%COMP%]{display:flex;flex-direction:column;justify-content:center;gap:8px;padding:24px 28px;width:260px;flex-shrink:0;background:var(--md-sys-color-surface-container-high);border-radius:14px;margin:12px 0 12px 12px;text-decoration:none;transition:background .18s ease}.ptp-mega-featured[_ngcontent-%COMP%]:hover{background:var(--md-sys-color-surface-container-highest)}.ptp-mega-featured[_ngcontent-%COMP%]:hover   .ptp-mega-featured__cta[_ngcontent-%COMP%]{gap:8px}.ptp-mega-featured__icon[_ngcontent-%COMP%]{width:48px;height:48px;border-radius:14px;background:var(--md-sys-color-primary);display:flex;align-items:center;justify-content:center;color:var(--md-sys-color-on-primary);margin-bottom:4px}.ptp-mega-featured__icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:24px!important;width:24px!important;height:24px!important}.ptp-mega-featured__label[_ngcontent-%COMP%]{font-size:10px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--md-sys-color-primary)}.ptp-mega-featured__title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:18px;font-weight:700;line-height:1.3;color:var(--md-sys-color-on-primary-container)}.ptp-mega-featured__desc[_ngcontent-%COMP%]{font-size:13px;line-height:1.6;color:color-mix(in srgb,var(--md-sys-color-on-primary-container) 70%,transparent);margin:0;flex:1}.ptp-mega-featured__cta[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:4px;font-size:13px;font-weight:600;color:var(--md-sys-color-primary);transition:gap .15s ease;margin-top:8px}.ptp-mega-featured__cta[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:16px!important;width:16px!important;height:16px!important}.ptp-mega-divider[_ngcontent-%COMP%]{width:1px;background:#00000012;flex-shrink:0}.ptp-mega-links[_ngcontent-%COMP%]{flex:1;display:grid;grid-template-columns:repeat(3,1fr);grid-template-rows:repeat(2,1fr);gap:0;padding:12px var(--ptp-layout-page-padding) 12px 8px}.ptp-mega-links--1row[_ngcontent-%COMP%]{grid-template-rows:repeat(1,1fr)}.ptp-mega-links--2x2[_ngcontent-%COMP%]{grid-template-columns:repeat(2,1fr);grid-template-rows:repeat(2,1fr)}.ptp-mega-links--3row[_ngcontent-%COMP%]{grid-template-rows:repeat(3,1fr)}.ptp-mega-links[_ngcontent-%COMP%]:not(.ptp-mega-links--3row)   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(4), .ptp-mega-links[_ngcontent-%COMP%]:not(.ptp-mega-links--3row)   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(5), .ptp-mega-links[_ngcontent-%COMP%]:not(.ptp-mega-links--3row)   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(6){border-top:1px solid rgba(0,0,0,.06)}.ptp-mega-links--3row[_ngcontent-%COMP%]   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(4), .ptp-mega-links--3row[_ngcontent-%COMP%]   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(5), .ptp-mega-links--3row[_ngcontent-%COMP%]   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(6), .ptp-mega-links--3row[_ngcontent-%COMP%]   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(7), .ptp-mega-links--3row[_ngcontent-%COMP%]   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(8), .ptp-mega-links--3row[_ngcontent-%COMP%]   .ptp-mega-item[_ngcontent-%COMP%]:nth-child(9){border-top:1px solid rgba(0,0,0,.06)}.ptp-mega-item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:14px;padding:16px;border-radius:14px;text-decoration:none;transition:background .15s ease}.ptp-mega-item[_ngcontent-%COMP%]:hover{background:var(--md-sys-color-surface-container-high)}.ptp-mega-item[_ngcontent-%COMP%]:hover   .ptp-mega-item__icon[_ngcontent-%COMP%]{background:var(--md-sys-color-primary-container);color:var(--md-sys-color-primary);transform:scale(1.08)}.ptp-mega-item[_ngcontent-%COMP%]:hover   .ptp-mega-item__title[_ngcontent-%COMP%]{color:var(--md-sys-color-primary)}.ptp-mega-item__icon[_ngcontent-%COMP%]{flex-shrink:0;width:40px;height:40px;border-radius:12px;background:var(--md-sys-color-surface-container-high);display:flex;align-items:center;justify-content:center;color:var(--md-sys-color-on-surface-variant);transition:background .15s ease,color .15s ease,transform .15s ease}.ptp-mega-item__icon[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:20px!important;width:20px!important;height:20px!important}.ptp-mega-item__text[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:3px;min-width:0}.ptp-mega-item__title[_ngcontent-%COMP%]{font-size:13px;font-weight:600;color:var(--md-sys-color-on-surface);line-height:1.3;transition:color .15s ease}.ptp-mega-item__desc[_ngcontent-%COMP%]{font-size:12px;line-height:1.5;color:var(--md-sys-color-on-surface-variant)}.ptp-sidenav[_ngcontent-%COMP%]{width:280px}.ptp-sidenav-header[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;padding:16px 16px 8px;border-bottom:1px solid var(--md-sys-color-outline-variant);margin-bottom:8px}.ptp-sidenav-logo[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-weight:700;font-size:22px;color:var(--md-sys-color-on-surface)}.ptp-skip-link[_ngcontent-%COMP%]{position:absolute;top:-100%;left:0;padding:12px 24px;background:var(--md-sys-color-primary);color:var(--md-sys-color-on-primary);font-size:14px;font-weight:500;text-decoration:none;z-index:9999}.ptp-skip-link[_ngcontent-%COMP%]:focus{top:0}.ptp-hero[_ngcontent-%COMP%]{padding:96px 0 88px;background:var(--md-sys-color-surface);text-align:center}@media(max-width:599px){.ptp-hero[_ngcontent-%COMP%]{padding:56px 0 48px}}.ptp-hero-content[_ngcontent-%COMP%]{max-width:720px;margin:0 auto;display:flex;flex-direction:column;align-items:center;gap:24px}.ptp-hero-title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:clamp(32px,5vw,56px);line-height:1.1;font-weight:400;color:var(--md-sys-color-on-surface);margin:0}.ptp-hero-body[_ngcontent-%COMP%]{font-size:16px;line-height:1.6;color:var(--md-sys-color-on-surface-variant);margin:0;max-width:560px}.ptp-hero-actions[_ngcontent-%COMP%]{display:flex;gap:12px;flex-wrap:wrap;justify-content:center}.ptp-btn-hero-outline[_ngcontent-%COMP%]{min-height:48px;padding:0 24px;border-color:var(--md-sys-color-outline)!important}.ptp-btn-hero-filled[_ngcontent-%COMP%]{min-height:48px;padding:0 24px}.ptp-mosaic[_ngcontent-%COMP%]{width:100%;line-height:0;overflow:hidden}.ptp-mosaic[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{width:100%;height:auto;display:block}.ptp-section[_ngcontent-%COMP%]{padding:72px 0}@media(max-width:599px){.ptp-section[_ngcontent-%COMP%]{padding:40px 0}}.ptp-section--white[_ngcontent-%COMP%]{background-color:var(--md-sys-color-surface-container-lowest);--ptp-section-bg: var(--md-sys-color-surface-container-lowest)}.ptp-section--cream[_ngcontent-%COMP%]{background-color:var(--md-sys-color-surface);--ptp-section-bg: var(--md-sys-color-surface)}.ptp-section-header[_ngcontent-%COMP%]{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:40px;gap:16px;flex-wrap:wrap}.ptp-eyebrow[_ngcontent-%COMP%]{font-size:11px;font-weight:600;letter-spacing:.1em;color:var(--md-sys-color-primary);text-transform:uppercase;margin:0 0 4px}.ptp-section-title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:clamp(24px,3vw,36px);font-weight:400;color:var(--md-sys-color-on-surface);margin:0;line-height:1.2}.ptp-see-all-btn[_ngcontent-%COMP%]{white-space:nowrap;flex-shrink:0;height:40px;border-radius:20px!important;border-color:#00000014!important;font-size:13px}.ptp-carousel-outer[_ngcontent-%COMP%]{overflow:hidden;margin-right:calc(50% - 50vw)}.ptp-event-carousel[_ngcontent-%COMP%]{display:flex;gap:24px;overflow-x:auto;scroll-snap-type:x mandatory;scrollbar-width:none;-webkit-overflow-scrolling:touch;padding-right:var(--ptp-layout-page-padding)}.ptp-event-carousel[_ngcontent-%COMP%]::-webkit-scrollbar{display:none}.ptp-carousel-nav[_ngcontent-%COMP%]{display:flex;gap:4px;margin-top:16px}.ptp-event-card[_ngcontent-%COMP%]{flex:0 0 calc(40% - 24px);scroll-snap-align:start;background:var(--md-sys-color-surface-container-lowest);border:1px solid rgba(0,0,0,.08);border-radius:16px;overflow:hidden;box-shadow:0 1px 4px #0000000d;transition:box-shadow .2s ease,transform .2s ease;display:flex;flex-direction:column}.ptp-event-card[_ngcontent-%COMP%]:hover{box-shadow:0 4px 16px #00000017;transform:translateY(-2px)}@media(max-width:768px){.ptp-event-card[_ngcontent-%COMP%]{flex:0 0 calc(85% - 12px)}}.ptp-event-card__image[_ngcontent-%COMP%]{width:100%;aspect-ratio:16/8;background-size:cover;background-position:center;position:relative}.ptp-event-card__image[_ngcontent-%COMP%]   .ptp-label[_ngcontent-%COMP%]{position:absolute;bottom:16px;left:20px;-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}.ptp-event-card__image--1[_ngcontent-%COMP%]{background:linear-gradient(145deg,#d4b8a0,#a07858 40%,#6b4e36)}.ptp-event-card__image--2[_ngcontent-%COMP%]{background:linear-gradient(145deg,#c8c0b8,#8c8078 45%,#4a4240)}.ptp-event-card__image--3[_ngcontent-%COMP%]{background:linear-gradient(145deg,#b8c4c0,#6e8880 45%,#374540)}.ptp-event-card__image--4[_ngcontent-%COMP%]{background:linear-gradient(145deg,#c4b8c8,#806878 45%,#3d2e40)}.ptp-event-card__body[_ngcontent-%COMP%]{padding:28px;display:flex;flex-direction:column;gap:12px;flex:1}.ptp-event-card__title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:22px;font-weight:400;line-height:1.3;color:var(--md-sys-color-on-surface);margin:0}.ptp-event-card__desc[_ngcontent-%COMP%]{font-size:14px;line-height:1.65;color:var(--md-sys-color-on-surface-variant);margin:0;flex:1}.ptp-event-card__footer[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:6px 8px;margin-top:auto;padding-top:16px;border-top:1px solid rgba(0,0,0,.07)}.ptp-meta-item[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:5px;font-size:12px;font-weight:500;color:var(--md-sys-color-on-surface-variant);background:var(--md-sys-color-surface-container);padding:4px 12px;border-radius:100px}.ptp-meta-icon[_ngcontent-%COMP%]{font-size:14px!important;width:14px!important;height:14px!important}.ptp-label[_ngcontent-%COMP%]{display:inline-flex;align-items:center;height:26px;padding:0 12px;border-radius:100px;font-size:12px;font-weight:600;letter-spacing:.03em;text-transform:uppercase;width:fit-content}.ptp-label--rose[_ngcontent-%COMP%]{background:var(--md-sys-color-primary);color:var(--md-sys-color-on-primary)}.ptp-label--sage[_ngcontent-%COMP%]{background:var(--md-sys-color-tertiary-container);color:var(--md-sys-color-on-tertiary-container)}.ptp-label--taupe[_ngcontent-%COMP%]{background:var(--md-sys-color-secondary-container);color:var(--md-sys-color-on-secondary-container)}.ptp-label--outline[_ngcontent-%COMP%]{background:transparent;color:var(--md-sys-color-primary);border:1.5px solid var(--md-sys-color-primary)}.ptp-filter-bar[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:28px}.ptp-filter-bar[_ngcontent-%COMP%]   .mat-mdc-chip-set-stacked[_ngcontent-%COMP%], .ptp-filter-bar[_ngcontent-%COMP%]   .mdc-evolution-chip-set__chips[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:8px}.ptp-filter-chip[_ngcontent-%COMP%]{--mdc-chip-container-shape-radius: 100px;--mdc-chip-container-height: 36px;--mdc-chip-elevated-container-color: var(--md-sys-color-surface-container-lowest);--mdc-chip-label-text-color: var(--md-sys-color-on-surface-variant);--mdc-chip-label-text-size: 14px;--mdc-chip-label-text-weight: 500;--mdc-chip-outline-color: rgba(0,0,0,.08);--mdc-chip-outline-width: 1px;border:1px solid rgba(0,0,0,.08);transition:all .15s ease}.ptp-filter-chip.mat-mdc-chip-selected[_ngcontent-%COMP%], .ptp-filter-chip[aria-selected=true][_ngcontent-%COMP%]{--mdc-chip-elevated-selected-container-color: var(--md-sys-color-primary);--mdc-chip-selected-label-text-color: var(--md-sys-color-on-primary);border-color:transparent}.ptp-komunikaty-carousel[_ngcontent-%COMP%]{display:flex;gap:16px;overflow-x:auto;scroll-snap-type:x mandatory;scrollbar-width:none;-webkit-overflow-scrolling:touch;padding-right:var(--ptp-layout-page-padding);margin-bottom:0}.ptp-komunikaty-carousel[_ngcontent-%COMP%]::-webkit-scrollbar{display:none}.ptp-komunikat-card[_ngcontent-%COMP%]{flex:0 0 calc(25% - 16px);scroll-snap-align:start;background:var(--md-sys-color-surface-container-lowest);border:1px solid rgba(0,0,0,.08);border-radius:16px;padding:28px;display:flex;flex-direction:column;gap:14px;box-shadow:0 1px 4px #0000000d;transition:box-shadow .2s ease,transform .2s ease}.ptp-komunikat-card[_ngcontent-%COMP%]:hover{box-shadow:0 4px 16px #00000017;transform:translateY(-2px)}@media(max-width:1024px){.ptp-komunikat-card[_ngcontent-%COMP%]{flex:0 0 calc(50% - 8px)}}@media(max-width:599px){.ptp-komunikat-card[_ngcontent-%COMP%]{flex:0 0 calc(85% - 8px)}}.ptp-komunikat-card__title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:20px;font-weight:400;line-height:1.35;color:var(--md-sys-color-on-surface);margin:0}.ptp-komunikat-card__desc[_ngcontent-%COMP%]{font-size:14px;line-height:1.65;color:var(--md-sys-color-on-surface-variant);margin:0;flex:1;display:-webkit-box;-webkit-line-clamp:4;-webkit-box-orient:vertical;overflow:hidden}.ptp-komunikat-card__footer[_ngcontent-%COMP%]{margin-top:auto;padding-top:16px;border-top:1px solid rgba(0,0,0,.07);display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}.ptp-komunikat-card__btn[_ngcontent-%COMP%]{height:36px!important;min-height:36px!important;font-size:13px;border-radius:100px!important;border-color:#00000014!important}.ptp-pagination[_ngcontent-%COMP%]{display:flex;gap:4px;margin-top:8px}.ptp-training-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin-bottom:48px}@media(max-width:768px){.ptp-training-grid[_ngcontent-%COMP%]{grid-template-columns:1fr}}.ptp-training-card[_ngcontent-%COMP%]{border-radius:16px;overflow:hidden;aspect-ratio:1/1;position:relative;display:flex;align-items:flex-end}.ptp-training-card--1[_ngcontent-%COMP%]{background:linear-gradient(160deg,#2a2a2a,#1a1a1a)}.ptp-training-card--2[_ngcontent-%COMP%]{background:linear-gradient(160deg,#1e2a1e,#0f1a0f)}.ptp-training-card--3[_ngcontent-%COMP%]{background:linear-gradient(160deg,#1a1a2a,#0a0a1a)}.ptp-training-card__inner[_ngcontent-%COMP%]{padding:24px;width:100%;background:linear-gradient(to top,rgba(0,0,0,.7) 0%,transparent 100%)}.ptp-training-card__title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:18px;font-weight:400;color:#fff;margin:0}.ptp-quote[_ngcontent-%COMP%]{text-align:center;max-width:680px;margin:0 auto;padding:48px 0 16px;position:relative}.ptp-quote__mark[_ngcontent-%COMP%]{display:block;font-family:Playfair Display,Georgia,serif;font-size:120px;line-height:.6;color:var(--ptp-color-brand);opacity:.5;margin-bottom:24px;-webkit-user-select:none;user-select:none}.ptp-quote__text[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:clamp(22px,3vw,32px);font-style:italic;font-weight:400;line-height:1.55;color:var(--md-sys-color-on-surface);margin:0 0 32px;letter-spacing:-.01em}.ptp-quote__attribution[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:12px}.ptp-quote__line[_ngcontent-%COMP%]{width:32px;height:2px;background:var(--ptp-color-brand);border-radius:2px}.ptp-quote__cite[_ngcontent-%COMP%]{font-style:normal;font-size:13px;font-weight:500;letter-spacing:.08em;text-transform:uppercase;color:var(--md-sys-color-on-surface-variant)}.ptp-newsletter[_ngcontent-%COMP%]{background-color:var(--md-sys-color-surface-container-high)}.ptp-newsletter-inner[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr 1fr;gap:64px;align-items:center}@media(max-width:768px){.ptp-newsletter-inner[_ngcontent-%COMP%]{grid-template-columns:1fr;gap:32px}}.ptp-newsletter-left[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px}.ptp-newsletter__eyebrow[_ngcontent-%COMP%]{font-size:12px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:var(--md-sys-color-primary);margin:0}.ptp-newsletter__title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:clamp(24px,3vw,36px);font-weight:400;line-height:1.2;color:var(--md-sys-color-on-primary-container);margin:0}.ptp-newsletter__desc[_ngcontent-%COMP%]{font-size:14px;line-height:1.65;color:var(--md-sys-color-on-primary-container);opacity:.75;margin:0}.ptp-newsletter-right[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px}.ptp-newsletter__form[_ngcontent-%COMP%]{display:flex;border:1px solid rgba(0,0,0,.08);border-radius:100px;background:var(--md-sys-color-surface-container-lowest);overflow:hidden;padding:4px 4px 4px 20px}@media(max-width:599px){.ptp-newsletter__form[_ngcontent-%COMP%]{flex-direction:column;border-radius:16px;padding:12px;gap:8px}}.ptp-newsletter__input[_ngcontent-%COMP%]{flex:1;min-width:0;border:none;background:transparent;font-family:Inter,sans-serif;font-size:14px;color:var(--md-sys-color-on-surface);outline:none}.ptp-newsletter__input[_ngcontent-%COMP%]::placeholder{color:var(--md-sys-color-on-surface-variant)}.ptp-newsletter__btn[_ngcontent-%COMP%]{flex-shrink:0;border-radius:100px!important;height:40px;padding:0 20px;font-size:14px}.ptp-newsletter__note[_ngcontent-%COMP%]{font-size:11px;line-height:1.6;color:var(--md-sys-color-on-primary-container);opacity:.6;margin:0}.ptp-cta-section[_ngcontent-%COMP%]{background-color:var(--md-sys-color-primary);padding:96px 0;position:relative;overflow:hidden}.ptp-cta-deco[_ngcontent-%COMP%]{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}.ptp-cta-inner[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr 1fr;gap:80px;align-items:end;position:relative}.ptp-cta-left[_ngcontent-%COMP%], .ptp-cta-right[_ngcontent-%COMP%]{display:flex;flex-direction:column;justify-content:center}@media(max-width:768px){.ptp-cta-inner[_ngcontent-%COMP%]{grid-template-columns:1fr;gap:40px}}.ptp-cta-eyebrow[_ngcontent-%COMP%]{font-size:11px;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:#ffffff80;margin:0 0 16px}.ptp-cta-title[_ngcontent-%COMP%]{font-family:Playfair Display,Georgia,serif;font-size:clamp(36px,4vw,52px);font-weight:400;line-height:1.2;color:#fff;margin:0}.ptp-cta-desc[_ngcontent-%COMP%]{font-size:15px;line-height:1.75;color:#ffffffa6;margin:0 0 32px}.ptp-cta-actions[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:40px}.ptp-cta-btn--filled[_ngcontent-%COMP%]{background:#fff!important;color:var(--md-sys-color-primary)!important;border-radius:100px!important;height:48px;padding:0 28px;font-weight:500;font-size:14px;text-decoration:none;display:inline-flex;align-items:center;transition:opacity .15s ease}.ptp-cta-btn--filled[_ngcontent-%COMP%]:hover{opacity:.9}.ptp-cta-btn--outline[_ngcontent-%COMP%]{border:1.5px solid rgba(255,255,255,.4)!important;color:#fff!important;border-radius:100px!important;height:48px;padding:0 28px;font-weight:500;font-size:14px;text-decoration:none;display:inline-flex;align-items:center;transition:border-color .15s ease,background .15s ease}.ptp-cta-btn--outline[_ngcontent-%COMP%]:hover{border-color:#ffffffb3!important;background:#ffffff0f}.ptp-cta-details[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:10px;padding-top:32px;border-top:1px solid rgba(255,255,255,.12)}.ptp-cta-detail[_ngcontent-%COMP%]{display:flex;align-items:center;gap:10px;font-size:13px;color:#ffffff8c}.ptp-cta-detail[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:16px;width:16px;height:16px;color:#fff6}.ptp-footer[_ngcontent-%COMP%]{background-color:var(--md-sys-color-primary);padding:64px 0 0}.ptp-footer[_ngcontent-%COMP%]   .ptp-logo-text[_ngcontent-%COMP%]{color:var(--md-sys-color-on-primary)}.ptp-footer[_ngcontent-%COMP%]   .ptp-logo-text[_ngcontent-%COMP%]   em[_ngcontent-%COMP%]{color:#ffffff8c}.ptp-footer-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:48px;padding-bottom:56px}@media(max-width:904px){.ptp-footer-grid[_ngcontent-%COMP%]{grid-template-columns:1fr 1fr;gap:40px}}@media(max-width:599px){.ptp-footer-grid[_ngcontent-%COMP%]{grid-template-columns:1fr;gap:32px}}.ptp-footer-col--brand[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:flex-start;gap:16px}.ptp-footer-brand-name[_ngcontent-%COMP%]{font-size:12px;font-weight:600;letter-spacing:.04em;color:#ffffff80;margin:-8px 0 0}.ptp-footer-brand-desc[_ngcontent-%COMP%]{font-size:13px;line-height:1.7;color:#fff9;margin:0;max-width:320px}.ptp-footer-social[_ngcontent-%COMP%]{display:flex;gap:12px;margin-top:4px}.ptp-footer-social__link[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:center;width:40px;height:40px;border-radius:10px;background:#ffffff1a;transition:background .15s ease}.ptp-footer-social__link[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{width:18px;height:18px;fill:#ffffffbf;transition:fill .15s ease}.ptp-footer-social__link[_ngcontent-%COMP%]:hover{background:#fff3}.ptp-footer-social__link[_ngcontent-%COMP%]:hover   svg[_ngcontent-%COMP%]{fill:#fff}.ptp-footer-col[_ngcontent-%COMP%]{display:flex;flex-direction:column}.ptp-footer-col__title[_ngcontent-%COMP%]{font-family:Inter,sans-serif;font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:#fff6;margin:0 0 16px}.ptp-footer-col__title--spaced[_ngcontent-%COMP%]{margin-top:28px}.ptp-footer-col__list[_ngcontent-%COMP%]{list-style:none;padding:0;margin:0;display:flex;flex-direction:column;gap:2px}.ptp-footer-col__list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{display:inline-flex;align-items:center;min-height:36px;font-size:14px;color:#ffffffb3;text-decoration:none;transition:color .15s ease}.ptp-footer-col__list[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover{color:#fff}.ptp-footer-address[_ngcontent-%COMP%]{font-style:normal;display:flex;flex-direction:column;gap:10px;margin-bottom:24px}.ptp-footer-address__row[_ngcontent-%COMP%]{display:flex;align-items:flex-start;gap:10px;font-size:14px;color:#ffffffb3;line-height:1.5}.ptp-footer-address__row[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:18px!important;width:18px!important;height:18px!important;flex-shrink:0;margin-top:1px;color:#fff6}.ptp-footer-address__row[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{color:#ffffffb3;text-decoration:none}.ptp-footer-address__row[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover{color:#fff}.ptp-footer-ipa[_ngcontent-%COMP%]{padding:14px 16px;border:1px solid rgba(255,255,255,.15);border-radius:12px;background:#ffffff0f}.ptp-footer-ipa__label[_ngcontent-%COMP%]{font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:#fff6;margin:0 0 4px}.ptp-footer-ipa__logo[_ngcontent-%COMP%]{display:block;max-width:100%;height:auto;max-height:36px;object-fit:contain;object-position:left;filter:brightness(0) invert(1);opacity:.75}.ptp-footer-ipa__name[_ngcontent-%COMP%]{font-size:13px;font-weight:600;color:#ffffffbf;margin:0;line-height:1.4}.ptp-footer-divider[_ngcontent-%COMP%]{--mat-divider-color: rgba(255,255,255,.12)}.ptp-footer-bottom[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;padding:20px 0;gap:16px;flex-wrap:wrap}.ptp-footer-copy[_ngcontent-%COMP%]{color:#fff6;margin:0;font-size:12px}.ptp-footer-legal[_ngcontent-%COMP%]{display:flex;gap:24px}.ptp-footer-legal[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{font-size:12px;color:#fff6;text-decoration:none;transition:color .15s ease}.ptp-footer-legal[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover{color:#ffffffbf}"]})};yu(rc,av).catch(e=>console.error(e));
